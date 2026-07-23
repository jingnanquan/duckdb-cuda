#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_tpch_csv_test.py
====================
用修改版 duckdb 二进制，对生成的 CSV 数据集跑 TPC-H 查询，验证 BHJ（Bitmap HashJoin）
在 CSV 上既能正确执行，又能真正被优化器采用。

验证方法：
  1. 正确性：同一查询分别在 open_bitmap_join=false / true 下各跑一次，
     用 .mode csv 输出，逐字节对比结果文本。有 ORDER BY 的 TPC-H 查询结果确定。
  2. 是否启用 BHJ：open_bitmap_join=true 下 EXPLAIN 查询，grep 是否出现 Bitmap/BHJ。

用法：
    python b_idea/scripts/run_tpch_csv_test.py \
        --binary build/release/duckdb \
        --setup  data/tpch_sf1_csv_bitmap/setup.sql \
        --queries q03 q05 q09 q12
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

QUERY_DIR = Path("/data/workspace/database/graindb/third_party/dbgen/queries")


def run_sql(binary: Path, sql: str, timeout: int) -> str:
    header = ".mode csv\n.nullvalue NULL\n"
    proc = subprocess.run(
        [str(binary)], input=header + sql, capture_output=True,
        text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"duckdb exited {proc.returncode}:\n{proc.stderr}")
    return proc.stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True, type=Path)
    ap.add_argument("--setup", required=True, type=Path)
    ap.add_argument("--queries", nargs="+", default=["q03", "q05", "q09", "q12"])
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()

    setup = args.setup.read_text(encoding="utf-8")

    overall_ok = True
    for qname in args.queries:
        qfile = QUERY_DIR / f"{qname}.sql"
        if not qfile.exists():
            print(f"[SKIP] {qname}: {qfile} not found")
            continue
        qtext = qfile.read_text(encoding="utf-8").strip()

        # 关闭 BHJ：纯 hash join，作为正确基准
        sql_off = f"SET open_bitmap_join=false;\n{setup}\n{qtext}\n"
        # 开启 BHJ
        sql_on = f"SET open_bitmap_join=true;\n{setup}\n{qtext}\n"
        # EXPLAIN（开启 BHJ）
        qno = qtext.rstrip().rstrip(";")
        sql_explain = f"SET open_bitmap_join=true;\n{setup}\nEXPLAIN {qno};\n"

        try:
            out_off = run_sql(args.binary, sql_off, args.timeout)
            out_on = run_sql(args.binary, sql_on, args.timeout)
            explain = run_sql(args.binary, sql_explain, args.timeout)
        except RuntimeError as e:
            print(f"[FAIL] {qname}: {e}")
            overall_ok = False
            continue

        correct = (out_off == out_on)
        # BHJ 是否真正生效（EXPLAIN 中出现 "Bitmap Join: yes"，注意区分
        # "Bitmap Join: no (...)" 这类被跳过的字段）
        bhj_hit = any("bitmap join: yes" in line.lower() for line in explain.splitlines())
        # 打印 EXPLAIN 中所有含 bitmap 的行（含 yes/no，便于核对）
        bhj_lines = [ln for ln in explain.splitlines() if "bitmap join" in ln.lower()]

        status = "PASS" if correct else "FAIL"
        print(f"[{status}] {qname}: results_match={correct}, bhj_in_explain={bhj_hit}")
        if bhj_lines:
            for ln in bhj_lines[:6]:
                print(f"        EXPLAIN> {ln.strip()}")
        if not correct:
            overall_ok = False
            # 输出差异前若干行方便排查
            lo = out_off.splitlines()
            ln2 = out_on.splitlines()
            print(f"        off_lines={len(lo)} on_lines={len(ln2)}")
            for i in range(min(5, max(len(lo), len(ln2)))):
                a = lo[i] if i < len(lo) else "<none>"
                b = ln2[i] if i < len(ln2) else "<none>"
                if a != b:
                    print(f"        - {a}\n        + {b}")

    print("\n=== SUMMARY:", "ALL PASS" if overall_ok else "SOME FAIL", "===")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
