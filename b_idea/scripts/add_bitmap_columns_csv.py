#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_bitmap_columns_csv.py
=========================

add_bitmap_columns.py 的 CSV 版本（详见 §6.1 模块 M-S）。

将 TPC-H 的 **headerless CSV** 原表升级为 BHJ（Bitmap HashJoin）友好版本。
与 parquet 版本的核心差异：

  * 输入文件是无表头的 CSV（如 tpch_customer.csv），需要显式给出列 schema
    （见 TPCH_SCHEMA）才能正确命名/定型列，否则 DuckDB 会把列命名为
    column0.. 且类型被推断成 VARCHAR，导致 BHJ 的整型 key 检查失败。
  * 输出同样写成 headerless CSV，文件基名去掉了输入前缀（tpch_），使
    ResolveLogicalTableName 解析出的表名（ExtractBaseName）与 meta 里的
    table_name（customer / orders / ...）一致。
  * rowid / *_ref 列通过 COPY ... (FORMAT CSV, HEADER false) 追加，类型用
    UBIGINT（与 parquet 版保持一致）。

优化器 / 执行器对文件格式是透明的（bitmap_join_resolver /
bitmap_hash_join_executor 不依赖 parquet），所以只要 CSV 物理携带了所需列，
BHJ 就能在 CSV 上工作——本脚本的目的就是生成这样的 CSV。

依赖：python >= 3.8, duckdb python 包（pip install duckdb）。

CLI 示例：
    python b_idea/scripts/add_bitmap_columns_csv.py \
        --input  /data/workspace/database/graindb/duckdb_benchmark_data \
        --output /data/workspace/database/duckdb-cuda/data/tpch_sf1_csv_bitmap \
        --constraints b_idea/scripts/tpch_constraints.csv \
        --meta-out data/tpch_sf1_csv_bitmap/bitmap_join_meta.json

    # 强制把 orders 当作非稠密 PK（用于验证 CSV 上 _rowid/*_ref 隐藏列的端到端路径）：
    python b_idea/scripts/add_bitmap_columns_csv.py \
        --input  .../duckdb_benchmark_data --output .../tpch_sf1_csv_bitmap_nondense \
        --constraints b_idea/scripts/tpch_constraints.csv \
        --force-nondense orders
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "[FATAL] python `duckdb` package not found in: " + sys.executable + "\n"
        "  Install it for the current interpreter, e.g.:\n"
        "    " + sys.executable + " -m pip install duckdb\n"
    )
    raise


# ---------------------------------------------------------------------------
# 常量与日志
# ---------------------------------------------------------------------------

ROWID_COLUMN_NAME = "_rowid"   # 当 PK 不是稠密整数时新增的列名
REF_COLUMN_SUFFIX = "_ref"     # FK 表新增的列名后缀
META_VERSION = 1

logger = logging.getLogger("add_bitmap_columns_csv")


def _setup_logger(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# TPC-H 列 schema（headerless CSV 必须显式给出，否则列名/类型会被推断错）
# 顺序与 tpch.sql / dbgen 输出一致。
# ---------------------------------------------------------------------------

TPCH_SCHEMA: Dict[str, Dict[str, str]] = {
    "customer": {
        "c_custkey": "INTEGER", "c_name": "VARCHAR", "c_address": "VARCHAR",
        "c_nationkey": "INTEGER", "c_phone": "VARCHAR",
        "c_acctbal": "DECIMAL(15,2)", "c_mktsegment": "VARCHAR",
        "c_comment": "VARCHAR",
    },
    "lineitem": {
        "l_orderkey": "INTEGER", "l_partkey": "INTEGER", "l_suppkey": "INTEGER",
        "l_linenumber": "INTEGER", "l_quantity": "DECIMAL(15,2)",
        "l_extendedprice": "DECIMAL(15,2)", "l_discount": "DECIMAL(15,2)",
        "l_tax": "DECIMAL(15,2)", "l_returnflag": "VARCHAR",
        "l_linestatus": "VARCHAR", "l_shipdate": "DATE", "l_commitdate": "DATE",
        "l_receiptdate": "DATE", "l_shipinstruct": "VARCHAR",
        "l_shipmode": "VARCHAR", "l_comment": "VARCHAR",
    },
    "nation": {
        "n_nationkey": "INTEGER", "n_name": "VARCHAR", "n_regionkey": "INTEGER",
        "n_comment": "VARCHAR",
    },
    "orders": {
        "o_orderkey": "INTEGER", "o_custkey": "INTEGER", "o_orderstatus": "VARCHAR",
        "o_totalprice": "DECIMAL(15,2)", "o_orderdate": "DATE",
        "o_orderpriority": "VARCHAR", "o_clerk": "VARCHAR",
        "o_shippriority": "INTEGER", "o_comment": "VARCHAR",
    },
    "part": {
        "p_partkey": "INTEGER", "p_name": "VARCHAR", "p_mfgr": "VARCHAR",
        "p_brand": "VARCHAR", "p_type": "VARCHAR", "p_size": "INTEGER",
        "p_container": "VARCHAR", "p_retailprice": "DECIMAL(15,2)",
        "p_comment": "VARCHAR",
    },
    "partsupp": {
        "ps_partkey": "INTEGER", "ps_suppkey": "INTEGER", "ps_availqty": "INTEGER",
        "ps_supplycost": "DECIMAL(15,2)", "ps_comment": "VARCHAR",
    },
    "region": {
        "r_regionkey": "INTEGER", "r_name": "VARCHAR", "r_comment": "VARCHAR",
    },
    "supplier": {
        "s_suppkey": "INTEGER", "s_name": "VARCHAR", "s_address": "VARCHAR",
        "s_nationkey": "INTEGER", "s_phone": "VARCHAR",
        "s_acctbal": "DECIMAL(15,2)", "s_comment": "VARCHAR",
    },
}


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Constraint:
    pk_table: str
    pk_col: str
    fk_table: str
    fk_col: str


@dataclasses.dataclass
class PKBinding:
    pk_table: str
    pk_column: str
    rowid_column: str
    rowid_offset: int
    row_count: int

    def to_meta(self) -> dict:
        return {
            "pk_table": self.pk_table,
            "pk_column": self.pk_column,
            "rowid_column": self.rowid_column,
            "rowid_offset": self.rowid_offset,
            "row_count": self.row_count,
        }


@dataclasses.dataclass
class FKBinding:
    fk_table: str
    fk_column: str
    ref_column: str
    pk_table: str
    pk_column: str

    def to_meta(self) -> dict:
        return {
            "fk_table": self.fk_table,
            "fk_column": self.fk_column,
            "ref_column": self.ref_column,
            "pk_table": self.pk_table,
            "pk_column": self.pk_column,
        }


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _columns_literal(schema: Dict[str, str]) -> str:
    """生成 DuckDB 接受的 STRUCT 字面量：{'col': 'TYPE', ...}"""
    return "{" + ", ".join(f"'{k}': '{v}'" for k, v in schema.items()) + "}"


def load_constraints(csv_path: Path) -> List[Constraint]:
    constraints: List[Constraint] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for raw in reader:
            if not raw:
                continue
            first = raw[0].strip()
            if not first or first.startswith("#"):
                continue
            if first.lower() == "pk_table":
                continue
            if len(raw) < 4:
                raise ValueError(f"bad constraint row: {raw!r}")
            constraints.append(Constraint(
                pk_table=raw[0].strip(), pk_col=raw[1].strip(),
                fk_table=raw[2].strip(), fk_col=raw[3].strip(),
            ))
    if not constraints:
        raise ValueError(f"no constraints found in {csv_path}")
    return constraints


def get_columns(con, path: str, schema: Dict[str, str]) -> List[Tuple[str, str]]:
    """返回 CSV 文件在给定 schema 下的 (name, type) 列表（DESCRIBE）。"""
    q = f"DESCRIBE SELECT * FROM read_csv({_sql_str(path)}, header=false, columns={_columns_literal(schema)})"
    rows = con.execute(q).fetchall()
    return [(r[0], r[1]) for r in rows]


def has_column(cols: List[Tuple[str, str]], name: str) -> bool:
    return any(c[0] == name for c in cols)


def detect_dense_int_pk(con, path: str, schema: Dict[str, str], pk_col: str,
                        force_nondense: bool) -> Tuple[bool, int, int]:
    qcol = _quote_ident(pk_col)
    read_sql = f"read_csv({_sql_str(path)}, header=false, columns={_columns_literal(schema)})"

    if force_nondense:
        # 仍需要 row_count
        total = int(con.execute(f"SELECT COUNT(*) FROM {read_sql}").fetchone()[0])
        return False, 0, total

    type_row = con.execute(f"SELECT typeof({qcol}) FROM {read_sql} LIMIT 1").fetchone()
    if type_row is None:
        return False, 0, 0
    typ = (type_row[0] or "").upper()
    integer_types = (
        "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT",
        "INTEGER", "UINTEGER", "BIGINT", "UBIGINT", "HUGEINT",
    )
    is_int = any(t in typ for t in integer_types)

    cnt_row = con.execute(f"SELECT COUNT(*), COUNT({qcol}) FROM {read_sql}").fetchone()
    total, non_null = int(cnt_row[0]), int(cnt_row[1])
    if total == 0:
        return False, 0, 0
    if not is_int or non_null != total:
        return False, 0, total

    mm_row = con.execute(
        f"SELECT MIN({qcol}), MAX({qcol}), COUNT(DISTINCT {qcol}) FROM {read_sql}"
    ).fetchone()
    pk_min, pk_max, distinct = int(mm_row[0]), int(mm_row[1]), int(mm_row[2])
    is_dense = (
        distinct == total
        and (pk_max - pk_min + 1) == total
        and pk_min in (0, 1)
    )
    return is_dense, pk_min, total


# ---------------------------------------------------------------------------
# 核心处理流程
# ---------------------------------------------------------------------------


class BitmapPreprocessor:
    def __init__(self, input_dir: Path, output_dir: Path,
                 constraints: List[Constraint], force: bool, dry_run: bool,
                 threads: int, input_prefix: str, input_suffix: str,
                 force_nondense: set) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.constraints = constraints
        self.force = force
        self.dry_run = dry_run
        self.threads = threads
        self.input_prefix = input_prefix
        self.input_suffix = input_suffix
        self.force_nondense = force_nondense

        self.pk_done: Dict[Tuple[str, str], PKBinding] = {}
        self.fk_done: Dict[Tuple[str, str, str, str], FKBinding] = {}

        all_tables = set()
        for c in constraints:
            all_tables.add(c.pk_table)
            all_tables.add(c.fk_table)
        self.all_tables = sorted(all_tables)
        # 校验每个表都有已知 schema
        for t in self.all_tables:
            if t not in TPCH_SCHEMA:
                raise ValueError(f"no embedded schema for table '{t}'")

        # current_schema[table] = 输出文件当前已有的全部列（原始 + 已追加的 rowid/ref）
        self.current_schema: Dict[str, Dict[str, str]] = {}

        self.con = duckdb.connect()
        if threads > 0:
            self.con.execute(f"PRAGMA threads={threads}")

    # ---------- 路径 ----------

    def _input_csv(self, table: str) -> Path:
        p = self.input_dir / f"{self.input_prefix}{table}{self.input_suffix}"
        if not p.exists():
            raise FileNotFoundError(f"input csv not found: {p}")
        return p

    def _output_csv(self, table: str) -> Path:
        return self.output_dir / f"{table}{self.input_suffix}"

    def _read_sql(self, path: str, schema: Dict[str, str]) -> str:
        return f"read_csv({_sql_str(path)}, header=false, columns={_columns_literal(schema)})"

    def _ensure_output_copy(self, table: str) -> Path:
        src = self._input_csv(table)
        dst = self._output_csv(table)
        dst.parent.mkdir(parents=True, exist_ok=True)
        # 初始化 current_schema 为原始 schema
        if table not in self.current_schema:
            self.current_schema[table] = dict(TPCH_SCHEMA[table])
        if dst.exists() and not self.force:
            logger.debug("[%s] reuse existing output csv: %s", table, dst)
            # 重新探测已有列以同步 current_schema
            try:
                cols = get_columns(self.con, str(dst), self.current_schema[table])
                for name, _ in cols:
                    if name not in self.current_schema[table]:
                        self.current_schema[table][name] = "UBIGINT"
            except Exception:
                pass
            return dst
        if self.dry_run:
            logger.info("[dry-run] would copy %s -> %s", src, dst)
            return dst
        logger.info("[%s] copy %s -> %s", table, src, dst)
        shutil.copy2(src, dst)
        return dst

    # ---------- PK ----------

    def process_pk_table(self, pk_table: str, pk_col: str) -> PKBinding:
        key = (pk_table, pk_col)
        if key in self.pk_done:
            return self.pk_done[key]

        out_path = self._ensure_output_copy(pk_table)
        forced = pk_table in self.force_nondense
        is_dense, base, row_count = detect_dense_int_pk(
            self.con, str(out_path), self.current_schema[pk_table], pk_col, forced
        )

        if is_dense:
            logger.info("[%s.%s] PK is dense int (base=%d, count=%d), reuse as rowid",
                        pk_table, pk_col, base, row_count)
            binding = PKBinding(pk_table=pk_table, pk_column=pk_col,
                                rowid_column=pk_col, rowid_offset=base,
                                row_count=row_count)
            self.pk_done[key] = binding
            return binding

        # 非稠密：追加 _rowid
        cols = get_columns(self.con, str(out_path), self.current_schema[pk_table]) \
            if out_path.exists() else []
        if has_column(cols, ROWID_COLUMN_NAME) and not self.force:
            logger.info("[%s] %s already exists, skip", pk_table, ROWID_COLUMN_NAME)
            row_count = int(self.con.execute(
                f"SELECT COUNT(*) FROM {self._read_sql(str(out_path), self.current_schema[pk_table])}"
            ).fetchone()[0])
        else:
            self._append_rowid_column(out_path, pk_table)
            row_count = int(self.con.execute(
                f"SELECT COUNT(*) FROM {self._read_sql(str(out_path), self.current_schema[pk_table])}"
            ).fetchone()[0])

        binding = PKBinding(pk_table=pk_table, pk_column=pk_col,
                            rowid_column=ROWID_COLUMN_NAME, rowid_offset=0,
                            row_count=row_count)
        self.pk_done[key] = binding
        return binding

    def _append_rowid_column(self, csv_path: Path, table: str) -> None:
        if self.dry_run:
            logger.info("[dry-run] would append %s to %s", ROWID_COLUMN_NAME, csv_path)
            return
        tmp_path = csv_path.with_suffix(".csv.tmp")
        qcol = _quote_ident(ROWID_COLUMN_NAME)
        schema = self.current_schema[table]
        sql = (
            f"COPY (SELECT *, CAST(row_number() OVER () - 1 AS UBIGINT) AS {qcol} "
            f"FROM {self._read_sql(str(csv_path), schema)}) "
            f"TO {_sql_str(str(tmp_path))} (FORMAT CSV, HEADER false)"
        )
        t0 = time.time()
        self.con.execute(sql)
        os.replace(tmp_path, csv_path)
        schema[ROWID_COLUMN_NAME] = "UBIGINT"
        logger.info("[%s] appended %s (took %.2fs)", table, ROWID_COLUMN_NAME,
                    time.time() - t0)

    # ---------- FK ----------

    def process_fk(self, constraint: Constraint, pk_binding: PKBinding) -> FKBinding:
        key = (constraint.fk_table, constraint.fk_col,
               constraint.pk_table, constraint.pk_col)
        if key in self.fk_done:
            return self.fk_done[key]

        ref_col = constraint.fk_col + REF_COLUMN_SUFFIX
        out_path = self._ensure_output_copy(constraint.fk_table)
        schema = self.current_schema[constraint.fk_table]
        cols = get_columns(self.con, str(out_path), schema) if out_path.exists() else []
        if has_column(cols, ref_col) and not self.force:
            logger.info("[%s] %s already exists, skip", constraint.fk_table, ref_col)
        else:
            self._append_ref_column(out_path, constraint.fk_table,
                                    constraint.fk_col, ref_col, pk_binding)
        binding = FKBinding(fk_table=constraint.fk_table, fk_column=constraint.fk_col,
                            ref_column=ref_col, pk_table=constraint.pk_table,
                            pk_column=constraint.pk_col)
        self.fk_done[key] = binding
        return binding

    def _append_ref_column(self, fk_path: Path, fk_table: str, fk_col: str,
                           ref_col: str, pk_binding: PKBinding) -> None:
        if self.dry_run:
            logger.info("[dry-run] would append %s to %s", ref_col, fk_path)
            return
        tmp_path = fk_path.with_suffix(".csv.tmp")
        qfk = _quote_ident(fk_col)
        qref = _quote_ident(ref_col)
        fk_schema = self.current_schema[fk_table]
        t0 = time.time()

        if pk_binding.rowid_column == pk_binding.pk_column:
            offset = pk_binding.rowid_offset
            expr = (f"CASE WHEN {qfk} IS NULL THEN NULL "
                    f"ELSE CAST({qfk} - {offset} AS UBIGINT) END AS {qref}")
            sql = (f"COPY (SELECT *, {expr} FROM {self._read_sql(str(fk_path), fk_schema)}) "
                   f"TO {_sql_str(str(tmp_path))} (FORMAT CSV, HEADER false)")
            self.con.execute(sql)
        else:
            pk_path = self._output_csv(pk_binding.pk_table)
            pk_schema = self.current_schema[pk_binding.pk_table]
            qpk_col = _quote_ident(pk_binding.pk_column)
            qpk_rowid = _quote_ident(pk_binding.rowid_column)
            sql = (
                f"COPY ("
                f"  SELECT f.*, CAST(p.{qpk_rowid} AS UBIGINT) AS {qref} "
                f"  FROM {self._read_sql(str(fk_path), fk_schema)} AS f "
                f"  LEFT JOIN {self._read_sql(str(pk_path), pk_schema)} AS p "
                f"    ON f.{qfk} = p.{qpk_col}"
                f") TO {_sql_str(str(tmp_path))} (FORMAT CSV, HEADER false)"
            )
            self.con.execute(sql)

        os.replace(tmp_path, fk_path)
        fk_schema[ref_col] = "UBIGINT"
        logger.info("[%s] appended %s -> %s.%s (took %.2fs)",
                    fk_table, ref_col, pk_binding.pk_table, pk_binding.rowid_column,
                    time.time() - t0)

    # ---------- 校验 ----------

    def validate_fk(self, fk_path: Path, ref_col: str, bitmap_size: int,
                    fk_table: str) -> None:
        if self.dry_run:
            return
        qref = _quote_ident(ref_col)
        schema = self.current_schema[fk_table]
        row = self.con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {qref} IS NOT NULL AND "
            f"({qref} < 0 OR {qref} >= {bitmap_size})) "
            f"FROM {self._read_sql(str(fk_path), schema)}"
        ).fetchone()
        bad = int(row[0])
        if bad > 0:
            raise RuntimeError(
                f"[VALIDATE-FAIL] {fk_path}.{ref_col} has {bad} out-of-range refs "
                f"(bitmap_size={bitmap_size})"
            )
        logger.debug("[validate] %s.%s ok", fk_path.name, ref_col)

    # ---------- 主入口 ----------

    def run(self) -> Tuple[List[PKBinding], List[FKBinding]]:
        seen_pk = set()
        for c in self.constraints:
            key = (c.pk_table, c.pk_col)
            if key in seen_pk:
                continue
            seen_pk.add(key)
            self.process_pk_table(c.pk_table, c.pk_col)

        pk_tables = {c.pk_table for c in self.constraints}
        for c in self.constraints:
            if c.fk_table not in pk_tables:
                self._ensure_output_copy(c.fk_table)

        for c in self.constraints:
            pk_binding = self.pk_done[(c.pk_table, c.pk_col)]
            fk_binding = self.process_fk(c, pk_binding)
            self.validate_fk(fk_path=self._output_csv(c.fk_table),
                             ref_col=fk_binding.ref_column,
                             bitmap_size=pk_binding.row_count,
                             fk_table=c.fk_table)

        return list(self.pk_done.values()), list(self.fk_done.values())


# ---------------------------------------------------------------------------
# meta JSON 输出
# ---------------------------------------------------------------------------


def write_meta_json(pk_bindings, fk_bindings, meta_path: Path,
                    input_dir: Path, output_dir: Path) -> None:
    meta = {
        "version": META_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "rowid_column_default": ROWID_COLUMN_NAME,
        "ref_column_suffix": REF_COLUMN_SUFFIX,
        "pk_bindings": [b.to_meta() for b in pk_bindings],
        "fk_bindings": [b.to_meta() for b in fk_bindings],
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    logger.info("wrote meta json: %s (%d PK, %d FK)",
                meta_path, len(pk_bindings), len(fk_bindings))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Add rowid / *_ref columns to TPC-H CSV for BHJ.")
    p.add_argument("--input", required=True, type=Path,
                   help="input CSV directory containing {tpch_}<table>.csv")
    p.add_argument("--output", required=True, type=Path,
                   help="output directory for enhanced CSV (will be created)")
    p.add_argument("--constraints", required=True, type=Path,
                   help="csv file describing PK/FK constraints")
    p.add_argument("--meta-out", type=Path, default=None,
                   help="path for output bitmap_join_meta.json "
                        "(default: <output>/bitmap_join_meta.json)")
    p.add_argument("--input-prefix", default="tpch_",
                   help="prefix of input csv file names (default: tpch_)")
    p.add_argument("--input-suffix", default=".csv",
                   help="suffix of input/output csv file names (default: .csv)")
    p.add_argument("--force-nondense", nargs="*", default=[],
                   help="force these PK tables to be treated as non-dense "
                        "(append _rowid + require FK *_ref join)")
    p.add_argument("--force", action="store_true", help="overwrite existing columns")
    p.add_argument("--dry-run", action="store_true", help="print plan only")
    p.add_argument("--threads", type=int, default=0, help="duckdb threads (0=default)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    _setup_logger(args.verbose)

    input_dir = args.input.resolve()
    output_dir = args.output.resolve()
    constraints_path = args.constraints.resolve()
    meta_path = (args.meta_out.resolve() if args.meta_out is not None
                 else output_dir / "bitmap_join_meta.json")
    force_nondense = set(args.force_nondense)

    if not input_dir.is_dir():
        logger.error("input dir not found: %s", input_dir)
        return 2
    if not constraints_path.is_file():
        logger.error("constraints file not found: %s", constraints_path)
        return 2

    constraints = load_constraints(constraints_path)
    logger.info("loaded %d constraints from %s", len(constraints), constraints_path)

    output_dir.mkdir(parents=True, exist_ok=True)

    pre = BitmapPreprocessor(
        input_dir=input_dir, output_dir=output_dir, constraints=constraints,
        force=args.force, dry_run=args.dry_run, threads=args.threads,
        input_prefix=args.input_prefix, input_suffix=args.input_suffix,
        force_nondense=force_nondense,
    )
    t0 = time.time()
    pk_bindings, fk_bindings = pre.run()
    logger.info("preprocess done in %.2fs", time.time() - t0)

    if not args.dry_run:
        write_meta_json(pk_bindings, fk_bindings, meta_path,
                        input_dir, output_dir)
    else:
        logger.info("[dry-run] would write meta json to %s", meta_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
