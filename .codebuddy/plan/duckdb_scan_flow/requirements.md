# 需求文档

## 引言

本文档定义一项 DuckDB `scan` 流程源码调研与技术文档整理任务的需求。目标是围绕 DuckDB 在执行阶段遇到 `scan operator` 后如何从 Parquet 文件或内存数据中读取数据、如何利用元信息做提前过滤、以及不同运行/存储模式下的内存与磁盘策略进行严谨说明。

调研范围应严格聚焦于 `scan` 操作符相关逻辑，必要时少量涉及 `executor` 与 `pipeline` 的执行衔接，不展开执行计划生成过程；文件格式仅关注 Parquet，不覆盖 CSV、原生 `.db` 存储格式以外的其他外部格式。最终产物应是一份完整、结构化、可阅读的中文技术文档，并配合必要代码片段解释关键路径。

## 需求

### 需求 1：说明执行计划进入 Scan Operator 后的 Parquet 扫描主流程

**用户故事：** 作为一名数据库内核学习者，我希望了解 DuckDB 在拿到执行计划后遇到 `scan operator` 时如何扫描 Parquet 文件，以便理解数据从硬盘文件进入内存执行单元的完整路径。

#### 验收标准

1. WHEN 文档描述 DuckDB 执行阶段的 `scan operator` THEN 文档 SHALL 说明执行器或 pipeline 如何触发 scan 相关接口，而不深入展开执行计划生成逻辑。
2. WHEN 文档描述 Parquet 扫描 THEN 文档 SHALL 说明从 Parquet 文件、row group、column reader 到内存中的执行数据结构之间的关键数据流。
3. WHEN 文档引用源码 THEN 文档 SHALL 至少包含若干关键函数、类或文件路径，并用短代码片段解释核心调用关系。
4. IF 某些执行细节依赖版本或实现细节 THEN 文档 SHALL 明确说明分析基于当前代码库，并避免泛化为所有 DuckDB 版本。
5. IF 某段逻辑属于非 Parquet 或非 scan 核心路径 THEN 文档 SHALL 排除或仅用一句话说明不在本次范围内。

### 需求 2：说明内存模式或已在内存中的表如何被识别并加速 Scan

**用户故事：** 作为一名性能优化分析者，我希望知道 DuckDB 在内存模式运行或表数据已在内存中时如何识别并直接扫描内存数据，以便判断内存表扫描与文件扫描的路径差异。

#### 验收标准

1. WHEN 文档讨论内存模式 scan THEN 文档 SHALL 区分外部 Parquet 文件扫描、内存中临时/普通表扫描、以及持久化数据库中已加载或缓存数据的概念边界。
2. WHEN 表数据已经位于内存 THEN 文档 SHALL 说明 scan 操作如何定位内存中的数据结构并从中产生执行用的数据块。
3. IF DuckDB 不会把 Parquet 文件扫描结果自动物化为完整内存表 THEN 文档 SHALL 明确说明这一点，避免将“缓存”“缓冲区”“表存储”混为一谈。
4. IF 内存扫描与 Parquet 文件扫描经过不同 scan function 或 storage path THEN 文档 SHALL 对比说明两者关键路径与性能差异来源。

### 需求 3：说明 Parquet 盘内过滤与提前裁剪机制

**用户故事：** 作为一名查询性能调优人员，我希望了解 DuckDB 如何利用 Parquet 或表的元信息在读取硬盘数据前进行过滤，以便判断 predicate pushdown、row group pruning 和 column pruning 的收益。

#### 验收标准

1. WHEN 文档描述盘内过滤 THEN 文档 SHALL 说明可利用的元信息类型，例如列投影、统计信息、row group metadata、zone map 或 Parquet footer 中的相关信息。
2. WHEN 查询包含过滤条件 THEN 文档 SHALL 说明过滤条件如何被传递到 Parquet scan 并用于提前跳过不必要的数据读取。
3. WHEN 描述读取过程 THEN 文档 SHALL 区分“完全跳过 row group/列读取”和“读取后在内存中再过滤”两类行为。
4. IF 某类过滤依赖 Parquet 文件是否包含统计信息 THEN 文档 SHALL 说明缺失元信息时的退化策略。
5. IF 文档提到代码实现 THEN 文档 SHALL 包含关键逻辑的代码片段或伪调用链，说明过滤判断发生的位置。

### 需求 4：比较三种运行/存储模式下的表数据完整性与溢出策略

**用户故事：** 作为一名系统架构分析者，我希望理解 DuckDB 在内存内模式、持久化模式、以及内存溢出模式下如何管理表数据与中间结果，以便判断“内存中的某个表是否完整”以及后续 query 访问时的策略。

#### 验收标准

1. WHEN 文档讨论内存内模式 THEN 文档 SHALL 说明通过内存连接创建表并插入数据时，数据无持久化且主要由内存存储管理的行为。
2. WHEN 文档讨论持久化模式 THEN 文档 SHALL 说明连接数据库文件时，表数据的持久化存储、buffer/cache、以及 scan 时按需读取之间的关系。
3. WHEN 文档讨论内存溢出模式 THEN 文档 SHALL 说明 DuckDB 默认启用的溢出或临时目录机制适用于哪些对象，例如中间结果、排序、聚合、join 等，而不是简单等同于“把不常用的表整体换出”。
4. IF 持久化模式下某次查询只读取了表的一部分数据 THEN 文档 SHALL 说明这不意味着内存中形成了一个“残缺表 a”供后续 query 复用，而是下次 query 仍根据存储与 buffer 管理重新扫描所需数据。
5. IF 查询中所有表单独都能容纳进内存但整体执行超出内存限制 THEN 文档 SHALL 分析 DuckDB 是否以 operator/state/intermediate data 为粒度溢出，而不是以整表为粒度换出。
6. IF 单个表大于内存容量 THEN 文档 SHALL 说明 scan 是否以 vector/data chunk、row group 或 block 等粒度流式读取，并解释内存中只保留当前处理窗口的机制。
7. WHEN 文档回答“同一 query 或不同 query 再访问表 a 的策略” THEN 文档 SHALL 明确区分 base table、scan 输出 chunk、中间结果、cache/buffer 以及物化结果。

### 需求 5：比较库大小大于内存容量时持久化模式与内存模式被动溢出的执行路径和适用性

**用户故事：** 作为一名系统选型者，我希望在已知整个库大小大于内存容量时，比较使用持久化模式和内存模式被动溢出的区别，以便选择更稳定、更高效的运行方式。

#### 验收标准

1. WHEN 文档比较持久化模式与内存模式 THEN 文档 SHALL 从数据加载路径、scan 路径、buffer 管理、溢出对象、启动成本和重复查询行为等方面进行对比。
2. IF 数据库总体大小超过内存容量 THEN 文档 SHALL 说明持久化模式通常如何按需读取与缓存，而不是要求一次性把全库加载到内存。
3. IF 内存模式依赖被动溢出 THEN 文档 SHALL 说明其限制、风险和可能的额外开销，尤其是 base table 数据与执行中间结果的区别。
4. WHEN 文档给出建议 THEN 文档 SHALL 明确说明在大于内存的数据集场景下哪种模式更适合，并给出理由与前提条件。
5. IF 建议存在例外场景 THEN 文档 SHALL 说明例如临时分析、小数据热集、一次性导入后计算等可能影响选择的因素。

### 需求 6：说明 DuckDB 默认 Parquet 配置与兼容性

**用户故事：** 作为一名数据工程师，我希望了解 DuckDB 默认 Parquet 格式相关配置及兼容能力，以便判断不同压缩格式、row group 设置的 Parquet 文件能否被读取或写出。

#### 验收标准

1. WHEN 文档描述默认 Parquet 配置 THEN 文档 SHALL 覆盖 DuckDB 读取和写出 Parquet 时可见的关键默认参数，例如压缩、row group size、统计信息、编码或相关选项。
2. WHEN 文档描述兼容性 THEN 文档 SHALL 说明 DuckDB 对不同压缩格式、不同 row group 大小、不同列编码或统计信息存在/缺失情况的兼容策略。
3. IF 某些配置只影响写出而不影响读取 THEN 文档 SHALL 明确区分 reader 兼容能力与 writer 默认行为。
4. IF 某些压缩格式需要编译选项或依赖库支持 THEN 文档 SHALL 标注可能的限制或检查方式。
5. WHEN 文档引用配置来源 THEN 文档 SHALL 尽量引用源码、官方配置定义或 README 中可验证的位置。

### 需求 7：控制源码阅读范围并形成可维护的中文技术文档

**用户故事：** 作为一名代码审阅者，我希望调研过程控制在必要源码范围内，并输出结构清晰的中文文档，以便高效获取结论而不被过多无关实现细节干扰。

#### 验收标准

1. WHEN 进行源码分析 THEN 文档 SHALL 优先关注 scan operator、Parquet reader、storage scan、buffer 或 vector/chunk 相关代码。
2. IF 需要解释执行衔接 THEN 文档 SHALL 仅少量引用 executor 或 pipeline 代码，不展开 planner、optimizer 或 binder 的完整逻辑。
3. IF 发现问题涉及 CSV、JSON、通用文件扫描或其他格式 THEN 文档 SHALL 不展开分析，除非用于对比 Parquet 的必要边界。
4. WHEN 输出最终文档 THEN 文档 SHALL 使用中文组织，包含目录化结构、关键结论、流程图或调用链、模式对比表和必要代码片段。
5. WHEN 给出结论 THEN 文档 SHALL 区分“源码可确认事实”“合理推断”和“仍需运行实验验证”的内容。
6. IF 某些问题无法仅凭有限源码阅读完全确认 THEN 文档 SHALL 明确列出不确定点与建议验证方法，而不是给出绝对化结论。
