#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_csv_setup.py
================
根据 add_bitmap_columns_csv.py 生成的 bitmap_join_meta.json，产出一份
setup.sql：为每个 TPC-H 表建立一个 VIEW，底层指向生成的 CSV（headerless，
用显式 columns schema 命名/定型列）。

这样就能直接拿 graindb/third_party/dbgen/queries/qNN.sql 原样跑——视图名
就是 customer / orders / ...，与 TPC-H 查询文本、以及 meta 的 table_name 一致。

用法:
    python b_idea/scripts/gen_csv_setup.py \
        --meta data/tpch_sf1_csv_bitmap/bitmap_join_meta.json \
        --out  data/tpch_sf1_csv_bitmap/setup.sql

随后用 duckdb 二进制：
    ./build/release/duckdb -c "PRAGMA bitmap_join_load('...meta.json'); SET open_bitmap_join=true; $(cat setup.sql) <query>"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 复用 CSV 脚本里的 TPC-H 原始列 schema 与列字面量构造器
sys.path.insert(0, str(Path(__file__).resolve().parent))
from add_bitmap_columns_csv import TPCH_SCHEMA, _columns_literal, ROWID_COLUMN_NAME  # noqa: E402


def build_full_schema(meta: dict, table: str) -> dict:
    schema = dict(TPCH_SCHEMA[table])
    for pk in meta["pk_bindings"]:
        if pk["pk_table"] == table and pk["rowid_column"] != pk["pk_column"]:
            schema[ROWID_COLUMN_NAME] = "UBIGINT"
    for fk in meta["fk_bindings"]:
        if fk["fk_table"] == table:
            schema[fk["ref_column"]] = "UBIGINT"
    return schema


def generate(meta_path: Path, out_path: Path, output_dir: Path) -> None:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    tables = []
    for pk in meta["pk_bindings"]:
        if pk["pk_table"] not in tables:
            tables.append(pk["pk_table"])
    for fk in meta["fk_bindings"]:
        if fk["fk_table"] not in tables:
            tables.append(fk["fk_table"])

    lines = []
    lines.append(f"-- auto-generated from {meta_path.name}")
    lines.append(f"PRAGMA bitmap_join_load({_sql(meta_path)});")
    lines.append("")
    for t in sorted(tables):
        schema = build_full_schema(meta, t)
        csv_path = output_dir / f"{t}.csv"
        cols = _columns_literal(schema)
        lines.append(
            f"CREATE OR REPLACE VIEW {t} AS\n"
            f"  SELECT * FROM read_csv({_sql(csv_path)}, header=false, columns={cols});"
        )
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out_path} ({len(tables)} views)")


def _sql(p: Path) -> str:
    s = str(p)
    return "'" + s.replace("'", "''") + "'"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meta", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--output-dir", type=Path, default=None,
                    help="CSV 目录（默认取 meta 里 output_dir 或 meta 同级目录）")
    args = ap.parse_args()

    meta = json.loads(args.meta.read_text(encoding="utf-8"))
    if args.output_dir is not None:
        output_dir = args.output_dir.resolve()
    elif meta.get("output_dir"):
        output_dir = Path(meta["output_dir"]).resolve()
    else:
        output_dir = args.meta.resolve().parent
    generate(args.meta, args.out.resolve(), output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
