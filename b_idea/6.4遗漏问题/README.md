---
title: BitmapJoin (BHJ) 6.4 遗漏问题 —— 任务拆分索引
version: v0.1.0
status: Draft（方案设计，尚未实施）
related:
  - ../6.4/6.4后续进展-实现与测试方案.md（条目1-5，已实施完成）
  - ../perf/sf5/tpch_prof.md
  - ../perf/sf5/tpch_q5_q9_q10.csv
  - ../../src/optimizer/bitmap_join_resolver.cpp
  - ../../src/optimizer/bitmap_join_resolver.md
  - ../../test/api/test_bitmap_join_chain.cpp
  - ../../test/api/test_bitmap_join_tpch.cpp
---

# 说明

条目1-5（`../6.4/6.4后续进展-实现与测试方案.md`）已全部实施并通过测试。在验收复盘时，发现了 4 个尚未解决的遗留问题，按"诊断 vs 实现"的性质拆分成两个任务：

| 原问题编号 | 内容 | 归入任务 |
| --- | --- | --- |
| 1 | 拿到 Q5/Q9/Q10 执行计划，分析命中率只有 3/5、3/5、1/3 的具体原因（无解 vs 可优化） | **任务1** |
| 2 | 对 Q5/Q9/Q10 的每个 HASH_JOIN 做三模式（baseline/perfect/bitmap）绝对耗时 profiling，判断优化效果不明显是计划问题还是执行器问题 | **任务1** |
| 3 | 优化"PK 表在中间 join 里落在 LEFT 侧"导致上层放弃 BHJ 的问题，需要在 `test_bitmap_join_chain.cpp` 补充测试 | **任务2** |
| 4 | `RIGHT_SEMI` 路径从未被测试跑过，需要补测 | **任务2** |

拆分理由：1、2 都是"不改变生产代码逻辑的诊断工作"（顶多加一个默认关闭的诊断标注），产出是分析报告和数据表，用来回答"3 是否值得投入"；3、4 都是"真实的代码改动 + 回归测试"，且 3 的实现方案设计依赖对现有约束（`RemoveUnusedColumns` 产生的绝对位置引用）的深入分析，篇幅和复杂度都明显大于诊断类工作，单独成文。

- 详见 [任务1-BHJ命中率归因与算子耗时画像.md](./任务1-BHJ命中率归因与算子耗时画像.md)（**已完成**：归因表+耗时表均已用真实 SF5 数据填充，并在此过程中意外发现并修复了一个真实的 `RIGHT_SEMI` 崩溃 bug——TPCH Q20）
- 详见 [任务2-中间Join左侧传播优化与RIGHT_SEMI覆盖.md](./任务2-中间Join左侧传播优化与RIGHT_SEMI覆盖.md)（**用户决策（v0.3.0）**：条目9/RIGHT_SEMI定案不做，不再补测试；条目8/LEFT侧传播待评估；新增条目8b——`CompressedMaterialization`与BHJ冲突的源头过滤方案，优先级高于条目8，已设计尚未实施）

## 任务1 核心结论速览（详见任务1文档）

1. **命中率归因**：Q5(3/5)/Q9(3/5)/Q10(1/3) 里"无解类"未命中各1个（多列等值条件，设计边界内）；"可优化类"里，Q5 的1个属于"中间join左侧限制"（任务2条目8目标），**但 Q9/Q10 的3个未命中，全部是一个新发现的、更大影响面的原因**——`CompressedMaterialization` 优化器给 join key 包了一层压缩函数，遮蔽了 `BitmapJoinResolver` 的追踪逻辑。已定案为任务2条目8b（源头过滤方案），优先于条目8。
2. **算子耗时画像**：已命中 BHJ 的 join 有真实的、可观的加速（部分降幅 75%~90%），但总耗时被未命中的大 join 主导，命中率不足是当前更大的瓶颈；同时发现一个"命中但反而变慢"的异常点（Q5），需要另开执行器层面的诊断任务。
3. **意外发现并修复的 bug**：22条标准TPC-H SQL的smoke test测出 Q20 在 `open_bitmap_join=true` 时崩溃——`RIGHT_SEMI` 类型join被`BitmapJoinResolver`错误地放行,但`BitmapJoinExecutor`只支持`INNER`。已修复（收紧`bhj_eligible`为仅`INNER`），22条SQL全部验证通过。经用户确认，正确支持`RIGHT_SEMI`不再投入（Q5/Q9/Q10均无此类join，无真实收益）。

## 建议执行顺序（v0.3.0 更新）

```text
任务1（诊断，已完成）
  ├─ 条目6：EXPLAIN 命中归因表 ──┐
  └─ 条目7：三模式算子耗时画像 ──┤
                                  ▼
                     结论：CompressedMaterialization冲突（条目8b）优先级 > LEFT侧限制（条目8）
                                  │
任务2（实现，待排期）
  ├─ 条目8b：CompressedMaterialization 源头过滤（优先，预期收益覆盖Q9+Q10共3个大/中型join）
  ├─ 条目8：中间 join LEFT 侧传播优化（次优先，仅覆盖Q5 1个join，待评估是否投入）
  └─ 条目9：RIGHT_SEMI —— 已决策不做（Q5/Q9/Q10均为INNER join，无真实场景驱动）
```

条目8b 改动集中在 `CompressedMaterialization`（`src/optimizer/compressed_materialization/compress_comparison_join.cpp`）一处，风险和工作量都明显小于条目8（不涉及物理执行层改动），建议优先落地。
