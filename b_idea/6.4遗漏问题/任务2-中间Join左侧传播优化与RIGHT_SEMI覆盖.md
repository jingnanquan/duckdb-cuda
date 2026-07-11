---
title: BitmapJoin (BHJ) 6.4 遗漏问题 —— 任务2：中间Join左侧传播优化与RIGHT_SEMI覆盖
version: v0.3.0
status: Draft（条目9已定案不做；条目8待评估；新增条目8b方案设计，尚未实施）
related:
  - ./README.md
  - ./任务1-BHJ命中率归因与算子耗时画像.md（条目8/8b投入优先级依赖其结论；§7.6记录了条目9相关的一个真实崩溃bug已被提前修复）
  - ../../src/optimizer/bitmap_join_resolver.cpp
  - ../../src/optimizer/bitmap_join_resolver.md
  - ../../src/optimizer/remove_unused_columns.cpp
  - ../../src/optimizer/late_materialization.cpp（ConstructRHS，思路参照）
  - ../../src/optimizer/compressed_materialization.cpp（条目8b核心改动点）
  - ../../src/optimizer/compressed_materialization/compress_comparison_join.cpp（条目8b核心改动点）
  - ../../src/optimizer/optimizer.cpp（RunBuiltInOptimizers，条目8b依赖的pass顺序证据）
  - ../../src/execution/physical_plan/plan_comparison_join.cpp
  - ../../src/execution/operator/join/physical_comparison_join.cpp
  - ../../src/execution/operator/join/bitmap_hash_join_executor.cpp（条目8b风险分析：稠密PK fast path）
  - ../../test/api/test_bitmap_join_chain.cpp
  - ../../test/api/test_bitmap_join_tpch_22queries.cpp（条目9探查证据来源）
---

# 说明

本任务对应用户提出的原问题 3、4，都是**实现类**任务：需要真实修改生产代码逻辑（条目8/8b）或补充此前完全没有覆盖到的代码路径的测试（条目9）。

> **v0.3.0 用户决策更新**：
> 1. **条目9（RIGHT_SEMI）定案：不做优化，不再补充单独测试**。理由：真实的 Q5/Q9/Q10 三条目标查询里完全没有 SEMI/RIGHT_SEMI 类型的 join；当前"收紧到仅 INNER、安全回退"的状态已经是正确、安全的终态（`test_bitmap_join_chain.cpp` 里已有的"BHJ safely falls back...for RIGHT_SEMI"回归测试足够覆盖这一契约，不需要再为"正确支持RIGHT_SEMI"补充新测试）。详见 §9.7。
> 2. **新增条目8b（`CompressedMaterialization` 与 BHJ 的冲突）**：用户明确了优先级——**查询性能提升是第一优先级，中间结果压缩是次要目标**；对于会影响 BHJ 命中的压缩，应该在源头过滤掉，而不是事后想办法让 BHJ 兼容压缩后的形态。任务1的诊断已经证明这个问题影响面（Q9 1个 + Q10 2个 = 3个大/中型join）比条目8（LEFT侧限制，仅Q5 1个）更大，本文档详细设计见 §8b。

---

> **历史记录（v0.2.0，条目9 Step A 的探查过程，现已被 §9.7 的定案取代）**：任务1条目7新增的"22条标准TPC-H SQL smoke test"在首次运行时，**真实复现了条目9原本要探查的风险**——TPCH Q20 的 `IN` 子查询去关联后产生 `RIGHT_SEMI` 类型的 join，`BitmapJoinResolver` 一直允许它进入 BHJ 候选，但 `BitmapJoinExecutor` 只支持 `INNER`，直接抛 `NotImplementedException` 而不是安全回退——这是一个真实的、会让查询直接报错的崩溃 bug，已在任务1推进过程中**发现并修复**：把 `bhj_eligible`（`BitmapJoinResolver::ResolveJoin` + `plan_comparison_join.cpp`）收紧为只允许 `JoinType::INNER`，不再允许 `RIGHT_SEMI`。修复后 22 条标准 SQL 全部通过，回归无破坏。详见任务1文档 §7.6。

---

## 条目8：中间 join「LEFT 侧传播」优化

### 8.1 问题重述

现有 `TraceBindingToGet`/`PropagateHiddenColumn`（`src/optimizer/bitmap_join_resolver.cpp:114-156`、`313-384`）只允许隐藏列 (`_rowid`/`*_ref`) 穿过中间 `LOGICAL_COMPARISON_JOIN` 的 **RIGHT** 侧向上传播；一旦隐藏列的源 `LogicalGet` 落在某个中间 join 的 **LEFT** 侧，直接放弃整条传播（`path_complete=false`），导致本来可以命中 BHJ 的**上层** join 也被连带放弃。

> **注意（任务1诊断结论）**：任务1条目6用真实 SF5 数据做归因后发现，Q5/Q9/Q10 三条查询里，**只有 Q5 的一个 join** 是被这个 LEFT 侧限制卡住的（`PATH_INCOMPLETE_LEFT_SIDE`），而 Q9/Q10 命中率不足的主因是另一个新发现的、更大影响面的问题——`CompressedMaterialization` 优化器包装 join key（见任务1文档 §6.5，建议新增为条目8b，优先级建议高于本条目）。本条目8仍然值得做，但预期收益（仅覆盖 Q5 一个 147ms 的 join）小于条目8b（覆盖 Q9+Q10 共3个大/中型 join，耗时占比更高）。

用户提出两个方向：
1. 下层 join 左侧传播会破坏位置时，"通知"上层 join 去更新位置；
2. 整个绑定过程改成自下而上，让下层 join 的隐藏列先"固化"，上层 join 绑定隐藏列时不需要再关心下层内部布局的变化。

并要求在 `test_bitmap_join_chain.cpp` 里补充能复现并验证这个场景的测试。

### 8.2 根因回顾（为什么现状只能走 RIGHT 侧）

`LogicalJoin::GetColumnBindings() = MapBindings(left, left_projection_map) ++ MapBindings(right, right_projection_map)`，输出布局**固定**为 `[left部分][right部分]` 的拼接，且这个布局早在我们的 pass 运行之前，就已经被 `RemoveUnusedColumns`/`ColumnLifetimeAnalyzer` 计算并"钉死"——**更上层某个祖先算子，可能已经用绝对位置引用了本 join 的 RIGHT 部分**（例如它自己的 `left_projection_map[k] = 7` 意味着"引用本 join 输出的第7列"）。

如果我们往 `left_projection_map` 追加一个新条目，这个新增列的绝对位置是 `left_projection_map.size()-1`，即 LEFT 部分的**末尾**——但由于输出布局是 `[left][right]`，LEFT 部分的末尾在**整个 join 输出**里的绝对位置，天然早于所有 RIGHT 部分的位置。也就是说：**LEFT 侧的"追加"，在 join 整体输出的坐标系里，其实是"插入在中间"**，会把 RIGHT 部分所有已存在的绝对位置全部 +1，从而静默破坏任何已经引用了这些位置的祖先节点。这正是开发条目4时在 TPCH Q5 真实复现过的崩溃根因。

而 RIGHT 侧追加是安全的，本质原因不是"RIGHT 侧特殊"，而是**RIGHT 部分恰好是整个输出布局的最后一段，追加在它末尾，等价于追加在整个 join 输出的物理最后**，不会导致后面任何东西移位（因为它后面什么都没有）。

**结论**：问题的本质不是"是 LEFT 还是 RIGHT"，而是"新增列有没有落在整个 join 输出的物理最后"。这个结论直接指向了下面的方案选择。

### 8.3 两个方向的可行性分析

**方向1（用户提出的"通知上层更新位置"）：定位并修正所有引用了被移位位置的祖先**

理论可行，但代价很高：一旦往 LEFT 侧插入新列，需要从这个中间 join 往上，对**每一个祖先算子**里"任何引用了 `≥ 插入点`位置"的地方做整体 `+1`；而这个移位本身还可能级联（某个祖先自己的 `left_projection_map` 需要移位之后，又会影响它的祖先）。这本质上是在事后局部重放一遍 `ColumnLifetimeAnalyzer` 的效果，正确性风险和实现复杂度都不低——历史上 `BitmapJoinOperatorState` 继承错误导致的 SIGSEGV 调试成本已经证明"贸然对已经跑完优化的绝对位置做二次修改"是高风险区域，**不建议采用方向1**。

**方向2（用户提出的"自下而上固化"）：现状其实已经是自下而上，但这不是问题所在**

`BitmapJoinResolver::VisitOperator` 本身就是"先递归子节点，再处理当前 join"（`bitmap_join_resolver.cpp:388-394`），已经是自底向上；如果中间 join 本身也是一个独立的 BHJ 候选，它会先完成自己的隐藏列传播，其 `left_projection_map`/`right_projection_map` 在外层 join 处理时已经是"固化"之后的状态。**"绑定顺序自下而上"这件事本身已经成立，不是问题根源**——真正的限制发生在"单次从 origin Get 到某个 consumer_join 的传播路径内部，跨越中间 join 时只能走 RIGHT 侧"，这与"先处理哪个 join"是两个独立的维度，方向2字面意义上不能单独解决 LEFT 侧问题。

**采用方案（在方向2的"消除移位"精神下，给出真正可行的实现）：旁路直通列**

不再往 `left_projection_map`/`right_projection_map` 里插入新条目，而是让隐藏列完全绕开"作为这个中间 join 的常规输出列参与`[left][right]`拼接"这条路，改为一个专门追加在**整个 join 输出物理最末尾**的旁路机制：给 `LogicalComparisonJoin` 增加

```cpp
// src/include/duckdb/planner/operator/logical_comparison_join.hpp
//! 条目8：需要穿透本 join 向上传递、但不参与本 join 任何条件/常规输出投影的"旁路"隐藏列。
//! 绑定在本 join 输出的物理最后一段（[left][right][bhj_passthrough_refs]），因此对
//! left_projection_map/right_projection_map 已经确定的绝对位置零侵入 —— 无论隐藏列源自
//! LEFT 还是 RIGHT 子树，都统一走这一条安全路径，从根本上消除"移位破坏祖先引用"的可能。
vector<unique_ptr<Expression>> bhj_passthrough_refs;
```

这样无论隐藏列源自哪一侧，传播算法都统一处理，`PropagationSide`/`side==LEFT就放弃` 的特判可以整体去掉，代码反而更简单。

### 8.4 实现方案（详细步骤）

**Step A：`LogicalComparisonJoin` 新增 `bhj_passthrough_refs` 字段**（见 8.3）。

**Step B：`GetColumnBindings()` / `ResolveTypes()` 追加旁路列**

需要找到 `LogicalComparisonJoin`（或其基类 `LogicalJoin`/`LogicalOperator`）里 `GetColumnBindings()`/`ResolveTypes()` 的现有实现，在计算完 `[left][right]` 之后，追加 `bhj_passthrough_refs` 对应的绑定/类型：

```cpp
// 伪代码，具体插入点需要先读一遍 LogicalComparisonJoin 现有的 GetColumnBindings/ResolveTypes 实现
vector<ColumnBinding> LogicalComparisonJoin::GetColumnBindings() {
    auto result = /* 现有 [left][right] 逻辑，不变 */;
    for (idx_t i = 0; i < bhj_passthrough_refs.size(); i++) {
        result.emplace_back(table_index, /* 某个从 result.size() 开始编号的 column_index 方案，
                                              需要与 ResolveTypes 保持一致 */);
    }
    return result;
}
```

> 需要先探查 `LogicalComparisonJoin::table_index` 目前的用途（普通 INNER/RIGHT_SEMI join 是否已经有一个自己的 `table_index`，还是只有 `LogicalProjection`/`LogicalGet` 才有自己的 `table_index`？如果 `LogicalComparisonJoin` 本身没有独立的 `table_index` 语义，需要评估旁路列的 `ColumnBinding` 应该用谁的 `table_index` 才能被下游正确识别——这是动手前必须确认的一处接口细节，不能想当然套用 `LogicalProjection` 的模式）。

**Step C：`PropagateHiddenColumn`/`TraceBindingToGet` 改造**

- `TraceBindingToGet` 里去掉"LEFT 侧命中 `path_complete=false`"的降级分支，LEFT/RIGHT 统一按同一逻辑记录 `PathStep`（`PropagationSide` 枚举可以整体删除，简化代码，见 `bitmap_join_resolver.cpp:114-156`）。
- `PropagateHiddenColumn` 里中间 join 分支（`bitmap_join_resolver.cpp:340-360`）统一改为：把当前 `binding` 包装成 `BoundColumnRefExpression`，`push_back` 进 `join.bhj_passthrough_refs`，新的 `binding` 更新为 `{join.table_index, join.GetColumnBindings().size()-1}`（即"本 join 输出的物理最后一列"），不再区分是从 LEFT 还是 RIGHT 侧传上来的。

**Step D：`ColumnBindingResolver` 认识 `bhj_passthrough_refs`**

参照条目3 Step D 已经验证过的"方式B"：让 `LogicalComparisonJoin::GetExpressions()`（`LogicalOperatorVisitor` 遍历用的官方接口）把 `bhj_passthrough_refs` 纳入遍历范围，`ColumnBindingResolver` 会自动把其中的 `BoundColumnRefExpression` 拍平成 `BoundReferenceExpression`，不需要特判改动 `ColumnBindingResolver` 本身。

**Step E：物理执行层——这是本方案相比条目3/4新增的、唯一有实质工作量的部分**

条目3/4 的隐藏列全程只涉及"逻辑层的列位置重排"：`Get`/`Projection`/`Filter`/中间 `join` 的 `projection_map` 都只是**选择/重排已经存在的列**，从未要求某个物理算子在执行期"额外拼接一列它原本不会输出的数据"。而条目8要求**中间 join 对应的物理算子**（如果它自己也会被规划成一个 `PhysicalHashJoin`/`PhysicalComparisonJoin`）在执行期，把 `bhj_passthrough_refs` 这一列从 build 或 probe 侧的输入 chunk **原样直通**到自己的输出 chunk 里——这是一块目前完全没有的物理执行能力。

具体需要探查：
1. `PhysicalComparisonJoin`/`PhysicalHashJoin` 现有的 `lhs_output_columns`/`rhs_output_columns` 机制（决定 join 输出里包含左/右两侧的哪些列）是否已经支持"这一列不参与任何 join 条件，只是原样传递"的语义——如果能直接把 `bhj_passthrough_refs` 转换成对应侧多加的一个 `output_column` 索引，成本会显著低于引入一套全新的物理拼接逻辑；
2. 如果 `lhs_output_columns`/`rhs_output_columns` 本身就是按"逻辑 binding → 物理下标"的方式工作的（大概率是这样，因为它们本就是 `RemoveUnusedColumns`/`ColumnLifetimeAnalyzer` 的产物），那么 `bhj_passthrough_refs` 拍平后的 `BoundReferenceExpression.index` 完全可以复用同一套机制，Step E 的成本可能远低于预估——**这一点必须先读代码求证，不能凭空假设**。

**Step E'（低成本降级路径，建议优先落地）**：在探查 Step E 之前，先注意到一个更廉价的特例——**当中间 join 的 `left_projection_map` 恰好为空**（即该 join 没有对左子树做任何列裁剪，`SELECT *` 风格或未触发 `RemoveUnusedColumns` 裁剪的场景，实测中很常见），此时 LEFT 子树的全部列本来就会原样出现在该 join 的输出里，**不需要新增任何列，也不需要 `bhj_passthrough_refs`**——只要把 `TraceBindingToGet`/`PropagateHiddenColumn` 里"LEFT 侧且 `left_projection_map` 为空"这一种情况单独放行（等价于现状 RIGHT 侧"`right_projection_map` 为空时的恒等透传"分支，`bitmap_join_resolver.cpp:353-354`，对称地为 LEFT 侧补一份），成本几乎为零，且能覆盖任务1条目6统计出来的"可优化类"里的一大部分（具体占比取决于条目6的归因结果）。

> **建议实施顺序**：先落地 Step E'（对称补齐"LEFT 侧且 `left_projection_map` 为空"的恒等透传特例，几乎不需要新增字段/物理执行改动），跑一次任务1的诊断脚本看命中率提升多少；再决定是否值得为"LEFT 侧且确实发生了列裁剪"这个更少见、成本更高（需要 Step A-E 的完整 `bhj_passthrough_refs` 机制）的场景继续投入。

### 8.5 测试方案（写入 `test_bitmap_join_chain.cpp`）

**Step 1：先写一个能稳定复现"LEFT 侧限制导致不命中"的测试（作为 bug 的可重现证明，在方案落地前就应该能跑通并断言"现状确实不命中"）**

```cpp
TEST_CASE("BHJ hidden column fails to propagate through the LEFT side of an intermediate "
          "INNER join (known limitation, 条目8 优化目标)",
          "[bitmap_join]") {
    RegistryResetGuard guard;
    auto &reg = BitmapJoinMetaRegistry::GetInstance();
    DuckDB db(nullptr);
    Connection con(db);
    SetupChainSchema(con, reg, /*with_nation=*/true);

    // 关键：让 customer（需要隐藏 crid 列）成为中间 join 的 LEFT 子树 —— 与现有
    // "BHJ hidden column propagates through an intermediate INNER join" 测试
    // （customer 写在 RIGHT，`nation JOIN customer`）刻意相反，这里写成
    // `customer JOIN nation`，并需要用 EXPLAIN 实测确认 BuildProbeSideOptimizer
    // 最终选择的 children[0]/children[1] 顺序是否真的把 customer 放在了 LEFT——
    // 如果代价估计导致它被自动挪去了 RIGHT，需要人为构造基数差异（例如给 nation
    // 加更多行，或用 PRAGMA disable_optimizer='build_side_probe_side' 固定形态）
    // 来确保测试稳定复现 LEFT 侧场景，而不是"凭感觉写SQL但实际根本没触发目标路径"。
    const string query = "SELECT count(*) AS cnt, sum(o.amt) AS sum_amt, 0 AS unused "
                          "FROM bhj_chain_orders o "
                          "JOIN (bhj_chain_customer c JOIN bhj_chain_nation n ON c.nk = n.nk) "
                          "ON o.ck = c.ck";

    REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
    auto plan = ExplainText(con, query);
    // 落地条目8方案之前：断言【不】命中，证明问题确实存在（反面教材）。
    // 落地条目8方案之后：这个断言要反转为 REQUIRE(StringUtil::Contains(plan, "Bitmap Join: yes"));
    // 并补充与 baseline 的结果一致性校验（复用 RunAgg3）。
}
```

**Step 2：条目8方案落地后，改写 Step 1 的断言为"必须命中"，并补充结果正确性校验**（与现有"BHJ hidden column propagates through an intermediate INNER join"测试的结构一致，`test_bitmap_join_chain.cpp:153-206`）。

**Step 3：回归现有测试**——条目4已有的"同名冲突"（`test_bitmap_join_chain.cpp:208-249`）、"幂等性"（`251-280`）两个测试，在改用 `bhj_passthrough_refs` 机制后必须继续全部通过，确认新机制不引入新的重复注入/冲突问题。

### 8.6 验收标准

1. 落地前：新增的"LEFT 侧限制"测试能稳定复现"确实不命中"（证明问题真实存在，且测试构造方式经过 EXPLAIN 实测验证，不是想象中的场景）；
2. 落地后（至少完成 8.4 Step E' 的低成本特例）：同一测试改为断言"命中"，结果与 baseline 一致；
3. 现有条目3/4 相关测试（`test_bitmap_join_chain.cpp` 全部3个 `TEST_CASE`、`test_bitmap_join_tpch.cpp`）全部保持通过；
4. 若完整实现了 `bhj_passthrough_refs`（Step A-E），额外验证：一个"确实发生了列裁剪的 LEFT 侧"场景（`left_projection_map` 非空）也能命中；
5. 用任务1条目6的诊断标注（`bhj_skip_reason`）重新跑一遍 Q5/Q9/Q10，确认 `PATH_INCOMPLETE_LEFT_SIDE` 归类的 join 数量下降，并用任务1条目7的耗时表确认总耗时是否随命中率提升而下降。

---

## 条目8b：`CompressedMaterialization` 与 BHJ 的冲突（新增，v0.3.0，用户明确要求评估）

### 8b.1 问题重述

用户明确了优先级原则：**查询性能提升是第一优先级，中间结果的大小压缩是次要目标**——对于任何"压缩优化"与"BHJ 优化"冲突、导致 BHJ 无法命中的场景，应该优先保证 BHJ 能命中（即从源头上把这类 join 排除在压缩的候选范围之外），而不是让压缩照常进行、再想办法让 BHJ 去兼容/看穿压缩后的形态。

任务1条目6已经用真实 SF5 数据定位了具体影响面：Q9 的 `[lineitem+part] ⋈ orders`（1个）、Q10 的 `[customer+nation] ⋈ [lineitem+orders]` 和 `lineitem ⋈ orders`（2个），一共 3 个未命中的 join，全部是因为 `CompressedMaterialization` 优化器把 join key 包了一层 `__internal_compress_integral_*` 压缩函数，导致 `BitmapJoinResolver::TraceBindingToGet` 在 `LOGICAL_PROJECTION` 分支处判定"非纯列引用"而放弃追踪。这三个 join 的耗时（Q9 的 `l_orderkey=o_orderkey` 约 1018ms、Q10 的 `c_custkey=o_custkey` 约 104ms + `l_orderkey=o_orderkey` 约 221ms）在各自查询总耗时里占比很大（任务1条目7 §7.3/§7.5），是当前命中率提升的最大单一潜在收益点。

### 8b.2 根因与执行时序证据（决定了"源头过滤"为什何可行）

关键证据来自 `src/optimizer/optimizer.cpp::RunBuiltInOptimizers()` 的 pass 执行顺序：

```text
...
BUILD_SIDE_PROBE_SIDE   （BuildProbeSideOptimizer：确定每个 join 的 build/probe 侧，即 children[0]/children[1] 及 join_type 的最终形态）
...
STATISTICS_PROPAGATION  （StatisticsPropagator::PropagateStatistics 逐节点调用；
                          CompressedMaterialization::Compress 在其内部对每个
                          LOGICAL_COMPARISON_JOIN 节点递归调用，即压缩发生在这里）
...
BITMAP_JOIN_RESOLVE     （BitmapJoinResolver，本任务的主角，全局最后一个 pass）
```

（完整顺序见 `src/optimizer/optimizer.cpp:207-337`）

**这意味着**：当 `CompressComparisonJoin`（`src/optimizer/compressed_materialization/compress_comparison_join.cpp`）处理某个 join 时，`BuildProbeSideOptimizer` 已经跑完——join 的 build（`children[1]`）/probe（`children[0]`）side、`join_type` 都已经是最终形态，和 `BitmapJoinResolver::ResolveJoin` 后续读到的完全一致。也就是说，**在压缩发生的那一刻，我们已经具备了判断"这个 join 是否是（或可能是）BHJ 候选"的全部信息**，不需要等 `BitmapJoinResolver` 跑完才能知道。这是"源头过滤"方案能够成立的关键前提。

### 8b.3 为什么"事后看穿"（诊断阶段的临时方案）不是长期正确的方案

任务1诊断阶段为了统计归因，给 `TraceBindingToGet`/`GetPlainColumnBinding` 加了 `IsCompressedMaterializationWrapper` 识别逻辑（当前**只用于诊断分类，没有实际"看穿并继续追踪"**）。如果考虑把这个识别逻辑升级成"看穿一层压缩、继续追踪、并让 BHJ 正常命中"，需要重新审视 `BitmapJoinExecutor`（`src/execution/operator/join/bitmap_hash_join_executor.cpp`）实际读取 join key 的两条路径，会发现看穿方案存在一个**依 PK 是否稠密而不同的隐蔽风险**：

- **稀疏 PK（如 `orders`，`rowid_column="_rowid"` != `pk_column="o_orderkey"`）**：BHJ 走的是"物化 `_rowid`/`*_ref` 隐藏列"路径（`bitmap_build_rowid_idx`/`bitmap_probe_ref_idx` 不为 `INVALID_INDEX`，见 `bitmap_hash_join_executor.cpp:256-260`）——这两个隐藏列是 `BitmapJoinResolver` 自己注入的全新物化列，从未被 `CompressedMaterialization` 处理过（`CompressedMaterialization` 运行在 `BitmapJoinResolver` 之前，那时隐藏列还不存在）。这种情况下，压缩包装只出现在原始 join key（`l_orderkey`/`o_orderkey`）的比较表达式上，**不影响** BHJ 实际读取的隐藏列——"看穿一层继续追踪"在这种情况下是安全的。
- **稠密 PK（如 `customer`/`part`/`supplier`，`rowid_column == pk_column`）**：BHJ 走的是 fast path，**直接读取 join key 本身的物化值**做 `rowid = key_value - rowid_offset`（`bitmap_hash_join_executor.cpp:259-260`，`rowid_offset = build_rowid_offset`；对照 `ExtractBuildRowids` 模板函数 `bitmap_hash_join_executor.cpp:85-110`）。如果这个 join key 恰好被 `CompressedMaterialization` 压缩过（类型变窄、做了 `value - min` 的偏移），fast path 读到的就是**压缩后的值**而不是原始 PK 值——`rowid = compressed_value - rowid_offset` 会计算出**错误的 rowid**，而不是抛异常。这是一个**静默产生错误结果**的正确性风险，比"追踪失败安全回退"严重得多，绝不能在没有专门改造 fast path（让它知道"这一列被压缩过，需要先解压，或者改用压缩后统计信息重新计算 offset"）之前just简单加一层"看穿并继续走 fast path"。

**结论**："事后看穿" 方案要想安全，必须区分稠密/稀疏两种情况分别处理，且稠密情况下改造成本和风险都不低（本质上是让 BHJ 执行器也理解压缩语义）。这与用户"性能优先、压缩次之"的排序矛盾——为了保留一个次要目标（压缩），反而要在核心目标（BHJ 性能优化）的执行器里引入新的复杂度和正确性风险。**源头过滤（不压缩 BHJ 候选 join 的 key）没有这个问题**：BHJ 该怎么追踪、怎么读取 join key，完全不需要改一行——因为压缩根本没发生。

### 8b.4 采用方案：在 `CompressComparisonJoin` 里跳过 BHJ 候选的 join key 压缩

**核心思路**：不修改 `BitmapJoinResolver`，而是让 `CompressedMaterialization::CompressComparisonJoin` 在决定"是否压缩某个 join 的等值条件列"之前，先做一次**轻量级的 BHJ 候选判断**（不需要完整复用 `ResolveJoin` 的追踪逻辑，只需要判断"这个 join 的等值条件是否*可能*是一个 BHJ 场景"），如果是，则跳过对该条件两侧列的压缩（保持普通 colref，不生成 compress 表达式），其余非 join-key 列的压缩逻辑不受影响。

**具体判断条件**（在 `CompressComparisonJoin` 现有的"`join.conditions.size()==1` 且两侧都是 `BOUND_COLUMN_REF`"分支内，`compress_comparison_join.cpp:56-84`）：

1. `join.join_type == JoinType::INNER`（复用 `bhj_eligible` 的判断之一，因为 `BuildProbeSideOptimizer` 已经跑完，此时读到的 `join_type` 就是最终值）；
2. `Settings::Get<OpenBitmapJoinSetting>(context)` 为 `true`（`open_bitmap_join` 关闭时，压缩完全不需要顾虑 BHJ，保持现状零影响——与 `BitmapJoinResolver` 本身的开关逻辑一致，见 `optimizer.cpp:332-336`）；
3. **不需要**（也不应该）在这里去查 `BitmapJoinMetaRegistry`、判断具体是不是登记过的 PK/FK——那是 `BitmapJoinResolver` 的职责，这里只需要"保守地"排除掉所有*可能*是 BHJ 场景的普通等值 INNER join，即便其中一部分最终因为其它原因（多列条件已经在外层被排除、或者未登记、或者 FK 落在 build 侧等）根本不会命中 BHJ——排除多了没有正确性代价，只有"少压缩了几个不会被压缩到的列"这一点点、可忽略的收益损失。

**改动点**（`src/optimizer/compressed_materialization/compress_comparison_join.cpp`，`CompressComparisonJoin` 函数内）：

```cpp
// 在现有 "join.conditions.size() == 1 ... both are bound column refs" 判断之前/之内增加：
if (join.conditions.size() == 1 && join.join_type == JoinType::INNER &&
    Settings::Get<OpenBitmapJoinSetting>(context)) {
    // 条目8b (b_idea/6.4遗漏问题 任务2): 用户明确要求"性能优先于压缩"——不对可能被
    // BitmapJoinResolver（本 pass 之后才运行）识别为 BHJ 候选的等值 join 条件列做压缩，
    // 从源头避免 TraceBindingToGet 因为一层 __internal_compress_integral_* 包装而放弃追踪
    // (real SF5 Q9/Q10, 3 个大/中型 join 受影响, 见任务1条目6/7)。保守排除：这里不查
    // BitmapJoinMetaRegistry（那是 BitmapJoinResolver 的职责），只要 join_type==INNER 且
    // open_bitmap_join=true 就跳过该条件两侧列的压缩 —— 排除多了没有正确性代价。
    goto skip_join_key_compression; // 或用 continue/提前 return 实现同等效果，具体看现有控制流
}
```

（伪代码用 `goto`/`continue` 只是示意"跳过压缩两侧 join key 列这一步"，具体实现需要先读一遍 `compress_comparison_join.cpp:56-88` 现有循环结构，找到最小改动点——大概率是在 `probe_compress_bindings.insert(lhs_colref.binding); continue;` 这一行之前直接 `continue`，让 `referenced_bindings` 正常收集这两个 binding（视为"被引用"，从而在 `TryCompressChild` 里被排除出压缩候选，走 `referenced_bindings` 已有的排除机制，不需要新增字段）。

**需要 include**：`compress_comparison_join.cpp` 目前没有 include `duckdb/main/settings.hpp`，需要新增；`Settings::Get<OpenBitmapJoinSetting>` 需要 `ClientContext`，`CompressedMaterialization` 类已经持有 `context`成员（`compressed_materialization.hpp:134`），直接可用。

### 8b.5 为什么不需要更复杂的方案

- **不需要**在 `CompressedMaterialization` 里引入 `BitmapJoinMetaRegistry` 依赖——`compressed_materialization.cpp`/`compress_comparison_join.cpp` 目前对 BHJ 完全无感知，保持这种解耦（只用 `join_type`+`open_bitmap_join` 这两个廉价、already-available 的信号做保守判断）风险最低，且不会在"BHJ resolver 逻辑今后如何演进"（比如条目8的 LEFT 侧优化落地后，追踪范围扩大）时需要同步维护两处重复逻辑。
- **不需要**像 8.3-8.4 的 `bhj_passthrough_refs` 方案那样动物理执行层——本条目完全在 `CompressedMaterialization` 这一个既有 pass 内部做一个"跳过压缩"的判断，改动量和风险都远小于条目8。

### 8b.6 测试方案

1. **单测/回归**：在 `test/optimizer/compressed_materialization.test_slow`（现有测试文件）或新增一个专门测试里，构造一个"等值 INNER join + `open_bitmap_join=true`"的场景，断言 `EXPLAIN` 里对应的 join key 列不再出现 `__internal_compress_integral_*`/`__internal_decompress_integral_*`；同时构造 `open_bitmap_join=false` 的对照场景，断言压缩**仍然发生**（确认没有把这个开关关闭时的现有压缩行为改坏）。
2. **端到端验证**：重跑任务1的诊断脚本/`test_bitmap_join_tpch_profile.cpp`，确认 Q9/Q10 的 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 分类清零，命中率从 Q9(3/5)/Q10(1/3) 提升，并用算子耗时表确认 Q9/Q10 总耗时随命中率提升下降（预期收益远大于条目8，见任务1 §7.5）。
3. **回归防护**：跑 `test/optimizer/compressed_materialization.test_slow`、`[bitmap_join]`、`[bitmap_join_tpch]`、`[bitmap_join_tpch_profile]`、`[join]` 全量确认无回归。

### 8b.7 验收标准

1. `CompressComparisonJoin` 在 `open_bitmap_join=true` 时不再压缩 `JoinType::INNER` 等值条件的两侧列；`open_bitmap_join=false` 时行为完全不变（现有压缩测试全部保持通过）；
2. Q9/Q10 的 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 归因项清零（用任务1条目6诊断标注重新验证）；
3. Q9/Q10 总耗时因命中率提升而下降，用任务1条目7的算子耗时表给出前后对比数据；
4. 全量回归（`compressed_materialization`/`bitmap_join`/`bitmap_join_tpch`/`join`）无破坏。

---

## 条目9：`RIGHT_SEMI` 路径测试补齐（Step A 已由任务1意外验证，问题已修复；剩余工作已收窄）

### 9.1 问题重述（原始状态）

`bhj_eligible`/`ResolveJoin`（`bitmap_join_resolver.cpp` 早期版本）曾显式允许 `RIGHT_SEMI` 类型的 join 走 BHJ：

```cpp
bool bhj_eligible = op.conditions.size() == 1 &&
                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
                    (op.join_type == JoinType::INNER || op.join_type == JoinType::RIGHT_SEMI);
```

但没有任何测试真正构造出一个 `RIGHT_SEMI` 类型的 `LogicalComparisonJoin` 去验证它——这是一段**从写下判断到当时从未被执行验证过**的代码路径。

### 9.2 结论已出：Step A 由任务1意外验证，且发现的是崩溃而非"仅仅未验证"

任务1条目7新增的"22条标准TPC-H SQL smoke test"首次运行时，**TPCH 标准 Q20** 就是一个稳定触发 `RIGHT_SEMI` 的真实场景：

```sql
SELECT s_name, s_address FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (
        SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
        AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem WHERE ...))
    AND s_nationkey = n_nationkey AND n_name = 'CANADA'
ORDER BY s_name;
```

其中 `s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ...)` 去关联后产生了 `RIGHT_SEMI` 类型的 `LogicalComparisonJoin`，`partsupp` 恰好又命中了 `bitmap_join_meta.json` 里登记的 PK/FK 关系（`partsupp.ps_suppkey` 是某个 PK 的 FK 引用），触发了 `BitmapJoinResolver` 把它当作候选处理——而 `BitmapJoinExecutor` 的构造函数从条目3/4开始就只实现了 `JoinType::INNER`（遇到别的类型 `throw NotImplementedException`），二者不一致直接导致查询报错：

```text
ERROR: Not implemented Error: BitmapJoinExecutor: only INNER joins are supported (MVP §6.4)
```

这回答了 9.4 原定验收标准第1条——**`RIGHT_SEMI` 能被标准 SQL（无需任何特殊构造，官方 TPC-H Q20 原文）稳定触达**，而且触达后果是**直接崩溃**，比"仅仅是未验证、可能语义不对"更严重。

### 9.3 已完成的修复（不等任务2排期，作为任务1推进过程中的阻断性问题立即处理）

把 `BitmapJoinResolver::ResolveJoin`（`bitmap_join_resolver.cpp`）和 `plan_comparison_join.cpp` 里的 `bhj_eligible` 判断，都从"`INNER` 或 `RIGHT_SEMI`"收紧为**只允许 `JoinType::INNER`**：

```cpp
// 两处保持一致
bool bhj_eligible = op.conditions.size() == 1 &&
                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
                    op.join_type == JoinType::INNER;
```

诊断枚举 `BitmapJoinSkipReason::NOT_INNER_OR_RIGHT_SEMI` 名字保留（兼容性），语义更新为"不是 INNER"。修复验证：

- 22条标准 TPC-H SQL 全部通过（`22 ok, 0 mismatched, 0 crashed`，此前是 `21 ok, 1 crashed`）；
- Q5/Q9/Q10 命中率、正确性不受影响（`[bitmap_join]`、`[bitmap_join_tpch]`、`test/sql/optimizer/*`、`[join]` 全量回归通过）。

详见任务1文档 §7.6。

### 9.4 剩余工作（范围已收窄）：是否要正确地支持 `RIGHT_SEMI`

当前状态（收紧到仅 `INNER`）已经是一个**安全、正确的终态**——`RIGHT_SEMI` 类型的 join（包括 TPCH Q20）现在会安全回退到普通 HashJoin，不会再崩溃，也不会产生错误结果。是否要在此基础上**进一步**支持 `RIGHT_SEMI` 走 BHJ（获得潜在性能收益），是一个独立的、可选的后续优化，需要先回答：

1. **`RIGHT_SEMI` 语义上"只输出 RIGHT 侧匹配到的行"，不产生笛卡尔积展开**——需要确认 `BitmapJoinExecutor` 现有的 Sink（build 侧建位图）/Probe（probe 侧查位图）逻辑，是否已经天然适配这种"只需要知道匹配与否，不需要真正输出 join 后的行"的语义，还是需要专门改造输出逻辑；
2. `ResolveJoin` 里"`cond.left`=probe/LHS，`cond.right`=build/RHS"的既有约定，在 `RIGHT_SEMI` 下 `children[0]`/`children[1]` 与"谁被去重、谁是探测方"的对应关系是否依然成立（`RIGHT_SEMI` 是"SEMI join 因为左右子树被交换而产生的镜像形式"，需要单独确认，不能想当然沿用 INNER 的约定）；
3. TPCH Q20 本身就是现成的、真实的验证场景，如果决定要支持，直接拿 Q20 做端到端验证即可，不需要再手工构造合成 SQL。

**建议**：由于当前"收紧到仅 INNER"已经是安全状态，这部分工作**优先级低于条目8/条目8b**（那两项能直接、可衡量地提升 Q5/Q9/Q10 命中率），可以作为任务2的可选扩展项，视排期决定是否投入。

### 9.5 需要补充的回归测试（防止未来重新放宽时重犯同样的错误）

在 `test_bitmap_join_chain.cpp` 补充一个"RIGHT_SEMI 安全回退"的回归测试，固化"当前状态"这条契约：

```cpp
TEST_CASE("BHJ safely falls back to regular hash join for RIGHT_SEMI (does not crash)", "[bitmap_join]") {
    RegistryResetGuard guard;
    auto &reg = BitmapJoinMetaRegistry::GetInstance();
    DuckDB db(nullptr);
    Connection con(db);
    SetupChainSchema(con, reg, /*with_nation=*/false);

    // o.ck IN (...) 对 bhj_chain_customer 的 IN 子查询去关联后应产生 RIGHT_SEMI/SEMI 类型的
    // join；即使 customer 的 ck 命中了 PK 绑定，BHJ 也必须安全跳过（NOT_INNER_OR_RIGHT_SEMI），
    // 不能抛异常。
    const string query = "SELECT count(*) FROM bhj_chain_orders o "
                          "WHERE o.ck IN (SELECT ck FROM bhj_chain_customer)";

    REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
    REQUIRE_NO_FAIL(*con.Query(query)); // 关键断言：不抛异常
}
```

### 9.6 验收标准（更新后）

- [x] "`RIGHT_SEMI` 能否被常见 SQL 稳定触达？"——**能**，TPCH Q20 是官方标准查询里的真实例子（已记录在本文档 §9.2）；
- [x] 触达后的风险已查清并修复：曾经是**直接崩溃**（`NotImplementedException`），已通过收紧 `bhj_eligible` 修复为安全回退；
- [x] 已补充 §9.5 的回归测试（`test_bitmap_join_chain.cpp` "BHJ safely falls back to a regular hash join for RIGHT_SEMI (does not crash)"），固化"RIGHT_SEMI 安全回退"这一契约；
- [x] 可选后续是否要正确支持 `RIGHT_SEMI`——**已决策：不做**（见 §9.7）。

### 9.7 最终决策（v0.3.0，用户拍板）：不做 RIGHT_SEMI 优化，不再补充单独测试

**决策**：`RIGHT_SEMI` 走 BHJ 的性能优化**不做**；§9.5 已有的"安全回退不崩溃"回归测试已经足够，**不再为"正确支持 RIGHT_SEMI"补充新的测试用例**。

**理由**：本轮任务的核心目标查询是 Q5/Q9/Q10——用 `EXPLAIN` 实测（任务1条目6归因表，§6.4）确认，这三条查询里**所有** `LogicalComparisonJoin` 都是 `INNER`，不存在任何 `SEMI`/`RIGHT_SEMI`。`RIGHT_SEMI` 唯一被真实观察到的触发场景是 TPCH 22条标准SQL里的 Q20（`IN` 子查询去关联），而 Q20 不在本轮性能优化的目标范围内。也就是说，即使投入成本去正确支持 `RIGHT_SEMI`（§9.4 列出的语义改造工作量不小：build/probe 约定重新审视、半连接输出语义改造），**收益也完全不会反映在 Q5/Q9/Q10 的任何一个指标上**——这是一个当前明确没有真实查询场景驱动的优化方向，不值得投入。

**现状即终态**：`bhj_eligible` 保持"仅 `JoinType::INNER`"；`RIGHT_SEMI`/`SEMI` 类型的 join 永远安全回退到普通 HashJoin，不崩溃、不产生错误结果（已有回归测试覆盖）。如果未来出现真实的、性能敏感的 `SEMI`/`RIGHT_SEMI` 查询场景，可以重新单独立项评估，届时 §9.4 的分析仍然有效，可以直接复用。

