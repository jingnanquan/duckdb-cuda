## DuckDB Scan 流程源码调研文档

### 0. 范围、版本与结论可信度

本文基于当前代码库 `/data/workspace/database/duckdb-cuda` 的源码进行分析，重点覆盖：

- `scan operator` 在执行阶段如何被 `PipelineExecutor` 触发。
- 外部 Parquet 文件如何从 footer/row group/column reader 读入 `DataChunk`。
- 普通表或内存表如何走 `seq_scan` + storage row group 路径。
- Parquet predicate pushdown、row group pruning、column pruning 与读取后过滤。
- 内存模式、持久化模式、临时 spill 的边界。
- Parquet 默认写出配置与读取兼容性。

不展开 planner/optimizer/binder 的完整过程；除必要对比外，不分析 CSV/JSON 等其他格式。

结论标记：

- **源码可确认事实**：能由当前源码直接验证。
- **合理推断**：源码边界清晰，但没有运行实验确认性能或策略细节。
- **需实验验证**：建议用 profiling、日志或构造数据集进一步确认。

### 1. 总体调用链：执行计划遇到 Scan Operator 后如何拉取数据

#### 1.1 Pipeline 从 source 拉取 `DataChunk`

执行阶段中，scan 通常是 pipeline 的 source。核心调用链是：

```text
PipelineExecutor::Execute
  -> PipelineExecutor::FetchFromSource
    -> PipelineExecutor::GetData
      -> pipeline.source->GetData(...)
        -> PhysicalTableScan::GetDataInternal
          -> TableFunction.function(context.client, data, chunk)
```

源码位置：

- [pipeline_executor.cpp](/data/workspace/database/duckdb-cuda/src/parallel/pipeline_executor.cpp)
- [physical_table_scan.cpp](/data/workspace/database/duckdb-cuda/src/execution/operator/scan/physical_table_scan.cpp)

关键代码片段：

```cpp
SourceResultType PipelineExecutor::FetchFromSource(DataChunk &result) {
    StartOperator(*pipeline.source);
    OperatorSourceInput source_input = {*pipeline.source_state, *local_source_state, interrupt_state};
    auto res = GetData(result, source_input);
    ...
    EndOperator(*pipeline.source, &result);
    return res;
}
```

```cpp
SourceResultType PhysicalTableScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
    ...
    TableFunctionInput data(bind_data.get(), l_state.local_state.get(), g_state.global_state.get());
    if (function.function) {
        function.function(context.client, data, chunk);
        ...
        if (chunk.size() > 0) {
            return SourceResultType::HAVE_MORE_OUTPUT;
        }
        return SourceResultType::FINISHED;
    }
    ...
}
```

**源码可确认事实**：`PhysicalTableScan` 本身不关心底层是 Parquet 文件还是 DuckDB 表存储；它只持有一个 `TableFunction`，真正扫描逻辑由 table function 实现。

### 2. Parquet 文件扫描主流程

#### 2.1 `parquet_scan` 注册为 table function

Parquet scan 入口在 [parquet_multi_file_info.cpp](/data/workspace/database/duckdb-cuda/extension/parquet/parquet_multi_file_info.cpp)。

```cpp
TableFunctionSet ParquetScanFunction::GetFunctionSet() {
    MultiFileFunction<ParquetMultiFileInfo> table_function("parquet_scan");
    ...
    table_function.filter_pushdown = true;
    table_function.filter_prune = true;
    table_function.late_materialization = true;
    return MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function));
}
```

这里启用了：

- **filter_pushdown**：过滤条件可以下推到 scan function。
- **filter_prune**：支持利用过滤条件做裁剪。
- **late_materialization**：先读过滤列，筛出 selection vector 后再读其他列。

#### 2.2 Parquet reader 的初始化

核心状态定义在 [parquet_reader.hpp](/data/workspace/database/duckdb-cuda/extension/parquet/include/parquet_reader.hpp)：

```cpp
struct ParquetReaderScanState {
    vector<idx_t> group_idx_list;
    int64_t current_group;
    idx_t offset_in_group;
    unique_ptr<CachingFileHandle> file_handle;
    unique_ptr<ColumnReader> root_reader;
    unique_ptr<TProtocol> thrift_file_proto;
    SelectionVector sel;
    unique_ptr<AdaptiveFilter> adaptive_filter;
    vector<ParquetScanFilter> scan_filters;
};
```

初始化过程在 [parquet_reader.cpp](/data/workspace/database/duckdb-cuda/extension/parquet/parquet_reader.cpp)：

```cpp
void ParquetReader::InitializeScan(ClientContext &context, ParquetReaderScanState &state,
                                   vector<idx_t> groups_to_read) const {
    state.current_group = -1;
    state.offset_in_group = 0;
    state.group_idx_list = std::move(groups_to_read);
    state.sel.Initialize(STANDARD_VECTOR_SIZE);
    ...
    state.thrift_file_proto = CreateThriftFileProtocol(context, *state.file_handle, state.prefetch_mode);
    state.root_reader = CreateReader(context);
    state.define_buf.resize(allocator, STANDARD_VECTOR_SIZE);
    state.repeat_buf.resize(allocator, STANDARD_VECTOR_SIZE);
}
```

`ParquetMultiFileInfo::TryInitializeScan` 中，当前实现对单个 reader 通常按 row group 分配 scan work：

```cpp
bool ParquetReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                      LocalTableFunctionState &lstate_p) {
    auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
    auto &lstate = lstate_p.Cast<ParquetReadLocalState>();
    if (gstate.row_group_index >= NumRowGroups()) {
        return false;
    }
    lstate.group_indexes = {gstate.row_group_index};
    gstate.row_group_index++;
    return true;
}
```

#### 2.3 从 Parquet row group 到 `DataChunk`

`ParquetReader::Scan` 的主逻辑：

1. 如果当前 row group 结束，切换到下一个 row group。
2. 对需要读取的列调用 `PrepareRowGroupBuffer`。
3. 每次最多读取 `STANDARD_VECTOR_SIZE` 行。
4. 如果有过滤条件，先读过滤列并生成 `SelectionVector`。
5. 对剩余列使用 `Select` 读取筛选后的行。
6. 输出 `DataChunk`。

关键代码：

```cpp
auto scan_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.offset_in_group);
result.SetCardinality(scan_count);
...
if (filters || deletion_filter) {
    ...
    child_reader.Filter(scan_count, define_ptr, repeat_ptr, result_vector,
                        scan_filter.filter, *scan_filter.filter_state,
                        state.sel, filter_count, is_first_filter);
    ...
    child_reader.Select(result.size(), define_ptr, repeat_ptr, result_vector, state.sel, filter_count);
    if (scan_count != filter_count) {
        result.Slice(state.sel, filter_count);
    }
} else {
    for (idx_t i = 0; i < column_ids.size(); i++) {
        auto rows_read = child_reader.Read(scan_count, define_ptr, repeat_ptr, result_vector);
        ...
    }
}
rows_read += scan_count;
state.offset_in_group += scan_count;
return SourceResultType::HAVE_MORE_OUTPUT;
```

**源码可确认事实**：Parquet scan 的输出单位是 `DataChunk`，每次读取上限受 `STANDARD_VECTOR_SIZE` 控制；并不是把整个 Parquet 文件一次性读入内存。

### 3. 内存表/普通表扫描路径，以及和 Parquet scan 的区别

#### 3.1 普通 DuckDB 表走 `seq_scan`

普通表 scan function 在 [table_scan.cpp](/data/workspace/database/duckdb-cuda/src/function/table/table_scan.cpp)：

```cpp
TableFunction TableScanFunction::GetFunction() {
    TableFunction scan_function("seq_scan", {}, TableScanFunc);
    scan_function.init_local = TableScanInitLocal;
    scan_function.init_global = TableScanInitGlobal;
    scan_function.projection_pushdown = true;
    scan_function.filter_pushdown = true;
    scan_function.filter_prune = true;
    scan_function.sampling_pushdown = true;
    scan_function.late_materialization = true;
    return scan_function;
}
```

`DuckTableScanState` 在初始化时拿到表的 storage：

```cpp
DuckTableScanState(ClientContext &context, const FunctionData *bind_data_p)
    : bind_data(bind_data_p->Cast<TableScanBindData>()),
      duck_table(bind_data.table.Cast<DuckTableEntry>()),
      tx(DuckTransaction::Get(context, duck_table.catalog)),
      storage(duck_table.GetStorage()),
      total_rows(storage.GetTotalRows()) {
}
```

scan 时调用：

```cpp
storage.Scan(tx, output, l_state.scan_state);
```

#### 3.2 Storage 层按 row group/vector 扫描

普通表 storage scan 的状态在 [scan_state.cpp](/data/workspace/database/duckdb-cuda/src/storage/table/scan_state.cpp)。

```cpp
bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
    while (row_group) {
        row_group->GetNode().Scan(TransactionData(transaction), *this, result);
        if (result.size() > 0) {
            return true;
        }
        ...
        row_group = GetNextRowGroup(*row_group).get();
        ...
    }
    return false;
}
```

`RowGroup::Scan` 在 [row_group.cpp](/data/workspace/database/duckdb-cuda/src/storage/table/row_group.cpp) 中按 vector 粒度扫描：

```cpp
idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row_group_row - current_row);
...
if (count == max_count && !has_filters) {
    for (idx_t i = 0; i < column_ids.size(); i++) {
        col_data.Scan(transaction, state.vector_index, state.column_scans[i], result.data[i]);
    }
} else {
    ...
    col_data.Filter(...);
    ...
    col_data.Select(...);
}
result.SetCardinality(count);
state.vector_index++;
```

#### 3.3 “内存模式”如何加速 scan？

需要区分三类概念：

| 场景 | scan function | 数据来源 | 是否自动形成完整内存表 |
|---|---|---|---|
| `read_parquet`/`parquet_scan` 外部文件 | `parquet_scan` | Parquet 文件 + footer/row group/page | 否 |
| `CREATE TABLE ... INSERT ...` 后扫描普通表 | `seq_scan` | `DuckTableEntry::GetStorage()` 中的 `DataTable`/row groups | 是，表自身就是 DuckDB storage 数据 |
| 持久化数据库中扫描表 | `seq_scan` | 持久化 block + buffer manager 按需 pin/read | 否，不要求全表驻留内存 |

**源码可确认事实**：DuckDB 不是在 `scan` 时判断“某个 Parquet 的扫描结果是否已经作为完整表在内存中”。外部 Parquet 与 DuckDB 表是两条不同 scan function/storage path：

- Parquet：`parquet_scan` -> `ParquetReader` -> `ColumnReader`。
- DuckDB 表：`seq_scan` -> `DataTable` -> `RowGroup`/`ColumnData`。

如果用户希望 Parquet 数据成为 DuckDB 内部表，需要显式执行类似：

```sql
CREATE TABLE t AS SELECT * FROM read_parquet('x.parquet');
```

之后再扫 `t`，走的是 `seq_scan`，不是继续走 Parquet reader。

### 4. Parquet 盘内过滤、提前裁剪与读取后过滤

#### 4.1 可利用的元信息

Parquet reader 会读取 footer 中的 `FileMetaData`，其中包含 schema、row groups、column chunk metadata、statistics 等。代码入口：

- `LoadMetadata`：读取 Parquet footer。
- `ReadStatisticsInternal`：聚合 row group/column stats。
- `GetPartitionStats`：将 row group 表示为 partition stats。

`ParquetReader::GetPartitionStats` 会为每个 row group 产生 `PartitionStatistics`：

```cpp
for (idx_t i = 0; i < metadata.row_groups.size(); i++) {
    auto &row_group = metadata.row_groups[i];
    PartitionStatistics partition_stats;
    partition_stats.row_start = offset;
    partition_stats.count = row_group.num_rows;
    partition_stats.count_type = CountType::COUNT_EXACT;
    ...
}
```

#### 4.2 Row group pruning：整组跳过

`PrepareRowGroupBuffer` 中会先读取 column statistics，再用 filter 检查是否可裁剪：

```cpp
if (filters) {
    auto stats = column_reader.Stats(state.group_idx_list[state.current_group], group.columns);
    auto filter_entry = filters->filters.find(col_idx);
    if (stats && filter_entry != filters->filters.end()) {
        auto &filter = *filter_entry->second;
        ...
        prune_result = filter.CheckStatistics(*stats);
        ...
        if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
            state.offset_in_group = group.num_rows;
            return;
        }
    }
}
state.root_reader->InitializeRead(...);
```

当 `FILTER_ALWAYS_FALSE` 时，`offset_in_group` 被置为 `group.num_rows`，后续该 row group 相当于被跳过。

#### 4.3 列裁剪与延迟物化

列裁剪发生在 `column_ids` 与 `projection_ids` 控制的读取列集合中。Parquet 的 `CreateReaderRecursive` 会根据 `column_indexes` 构建只需要的 reader。读取时只遍历 `column_ids`：

```cpp
for (idx_t i = 0; i < column_ids.size(); i++) {
    auto col_idx = MultiFileLocalIndex(i);
    PrepareRowGroupBuffer(state, col_idx);
    ...
}
```

有过滤条件时，先读过滤列，再读剩余列：

```cpp
child_reader.Filter(...);
need_to_read[local_idx.GetIndex()] = false;
...
child_reader.Select(..., state.sel, filter_count);
```

#### 4.4 读取后内存过滤

如果统计信息不足以跳过 row group，或者过滤条件无法由统计信息证明为 false，DuckDB 会读取过滤列并在内存中执行 filter，生成 `SelectionVector`。

**退化策略**：

- 没有 stats：不能做 row group pruning，继续读取。
- stats 不足以证明 false：继续读取过滤列并做内存过滤。
- 支持 bloom filter 且 bloom filter 排除：可进一步裁剪。

代码中 bloom filter 判断：

```cpp
if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE &&
    ParquetStatisticsUtils::BloomFilterExcludes(...)) {
    prune_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
}
```

### 5. 普通表 storage 的 zone map 过滤

普通 DuckDB 表不是 Parquet footer 元信息，而是 storage 层的 row group/column segment zonemap。

`RowGroup::InitializeScan` 会先做 row group 级 `CheckZonemap`：

```cpp
bool RowGroup::InitializeScan(CollectionScanState &state, SegmentNode<RowGroup> &node) {
    auto &filters = state.GetFilterInfo();
    if (!CheckZonemap(filters)) {
        return false;
    }
    ...
}
```

`RowGroup::Scan` 中还会对当前 vector/segment 再做 `CheckZonemapSegments`：

```cpp
if (!CheckZonemapSegments(state)) {
    continue;
}
```

**源码可确认事实**：普通表和 Parquet 都支持提前过滤，但元信息来源不同：

- Parquet：footer/row group/column chunk statistics/bloom filter。
- DuckDB 表：内部 row group/column segment statistics/zonemap。

### 6. 三种运行/存储模式下的数据完整性与 spill 策略

#### 6.1 内存内模式

内存数据库的 block manager 是 [in_memory_block_manager.hpp](/data/workspace/database/duckdb-cuda/src/include/duckdb/storage/in_memory_block_manager.hpp)。它明确禁止持久化 IO：

```cpp
void Read(QueryContext context, Block &block) override {
    throw InternalException("Cannot perform IO in in-memory database - Read!");
}
void Write(FileBuffer &block, block_id_t block_id) override {
    throw InternalException("Cannot perform IO in in-memory database - Write!");
}
bool InMemory() override {
    return true;
}
```

因此，在内存连接中创建表并插入数据后，base table 数据存在于 DuckDB 的内部 storage/buffer 管理结构中；数据库关闭后不持久化。

#### 6.2 持久化模式

持久化数据库由 [storage_manager.hpp](/data/workspace/database/duckdb-cuda/src/include/duckdb/storage/storage_manager.hpp) 描述：

```cpp
//! StorageManager is responsible for managing the physical storage of a persistent database.
class StorageManager { ... };

//! Stores the database in a single file.
class SingleFileStorageManager : public StorageManager { ... };
```

持久化表扫描仍然走 `seq_scan` -> `DataTable` -> `RowGroup`，但底层 column segment/block 可以按需通过 buffer manager pin/read。内存中缓存的是 block/buffer，不是一个语义上的“残缺表 a”。

#### 6.3 临时 spill/内存溢出模式

[standard_buffer_manager.hpp](/data/workspace/database/duckdb-cuda/src/include/duckdb/storage/standard_buffer_manager.hpp) 明确说明 buffer manager 支持将临时 buffer swap 到磁盘：

```cpp
//! ... offers configuration options specific to a database,
//! including whether to support swapping temp buffers to disk, and where to swap them to.
```

具体写临时 buffer：

```cpp
void StandardBufferManager::WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id, FileBuffer &buffer) {
    RequireTemporaryDirectory();
    ...
    temporary_directory.handle->GetTempFile().WriteTemporaryBuffer(block_id, buffer);
}
```

读回临时 buffer：

```cpp
unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBuffer(...) {
    ...
    auto buffer = temporary_directory.handle->GetTempFile().ReadTemporaryBuffer(...);
    ...
    return buffer;
}
```

没有临时目录时，报错提示也说明了边界：

```cpp
"Database is launched in in-memory mode and no temporary directory is specified.\n"
"Unused blocks cannot be offloaded to disk."
```

#### 6.4 回答“残缺表 a”问题

**源码可确认事实**：

- 持久化模式下，某次查询只读了表的一部分 row group 或 block，不表示内存里生成了一个供后续 query 复用的“残缺表 a”。
- 后续 query 会重新通过 `seq_scan` 根据自身 filters/projections 初始化 scan state，并通过 buffer manager 命中或重新读取需要的 block。
- Parquet scan 同理：过滤后输出的只是 pipeline 中流动的 `DataChunk`，不会自动物化成完整或残缺内存表。

#### 6.5 spill 粒度是不是整表？

**源码可确认事实**：buffer manager 管的是 `BlockHandle`/`FileBuffer`/temporary buffer，不是以“表”为单位整体换出。

`StandardBufferManager::RegisterMemory` 注释说明：

```cpp
//! Register an in-memory buffer of arbitrary size...
//! if true, it will be destroyed,
//! if false, it will be written to a temporary file so it can be reloaded
```

所以当内存不足时，溢出/eviction 更接近 **buffer/block/operator state/intermediate data** 粒度，而不是“不常用的表整体换出”。

对于单表超过内存容量的情况，scan 本身按 row group/vector/block 流式处理：

- Parquet：row group + `STANDARD_VECTOR_SIZE` chunk。
- 普通表：row group + vector + column segment。
- Buffer manager：按 block/buffer pin、evict、reload。

**合理推断**：只要执行算子不需要全量物化，单表大于内存也可以通过流式 scan 推进；真正可能 spill 的往往是 sort/hash aggregate/hash join/window 等中间状态，而不是 scan 输出的整张表。

### 7. 数据库大于内存时：持久化模式 vs 内存模式被动溢出

| 维度 | 持久化模式 `duckdb('file.db')` | 内存模式 + 临时 spill |
|---|---|---|
| base table 数据 | 天然在持久化 storage 中 | 初始在内存数据库 storage 中，关闭不保留 |
| scan 路径 | `seq_scan` 按需读 block/row group，buffer cache 命中则复用 | `seq_scan` 扫内存 storage；内存压力下只能 offload 可 eviction 的 buffer/temp data |
| Parquet 外部文件 | `parquet_scan` 直接读 Parquet，不要求导入 | 同左；除非显式 `CREATE TABLE AS` 导入内存库 |
| 重复查询 | 持久化数据稳定，buffer cache 可加速 | 数据需重新导入或保留进程；临时 spill 文件不是持久化表 |
| 大于内存场景 | 更自然：按需读 + cache + spill 中间结果 | 风险更高：base table 导入成本高，临时 spill 不是数据库持久化机制 |
| 推荐性 | **推荐** | 仅适合临时分析、小热集、一次性计算 |

**结论**：如果已知整个库大小大于内存容量，通常应选择持久化模式。它的设计前提就是 base table 可驻留在磁盘，scan 按需读取。内存模式的临时 spill 是缓解内存压力的执行/缓冲机制，不应被理解为“自动把内存数据库变成可靠的磁盘数据库”。

### 8. Parquet 默认配置与兼容性

#### 8.1 默认写出配置

默认值来自 [parquet_extension.cpp](/data/workspace/database/duckdb-cuda/extension/parquet/parquet_extension.cpp)：

```cpp
struct ParquetWriteBindData : public TableFunctionData {
    duckdb_parquet::CompressionCodec::type codec = duckdb_parquet::CompressionCodec::SNAPPY;
    idx_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
    idx_t row_group_size_bytes = NumericLimits<idx_t>::Maximum();
    optional_idx dictionary_size_limit;
    idx_t string_dictionary_page_size_limit = PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE;
    bool enable_bloom_filters = true;
    double bloom_filter_false_positive_ratio = 0.01;
    int64_t compression_level = ZStdFileSystem::DefaultCompressionLevel();
    ParquetVersion parquet_version = ParquetVersion::V1;
};
```

`DEFAULT_ROW_GROUP_SIZE` 定义在 [storage_info.hpp](/data/workspace/database/duckdb-cuda/src/include/duckdb/storage/storage_info.hpp)：

```cpp
#define DEFAULT_ROW_GROUP_SIZE 122880ULL
```

可见默认写出参数包括：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| compression/codec | `SNAPPY` | 默认写 Parquet 使用 Snappy |
| row_group_size | `122880` 行 | `DEFAULT_ROW_GROUP_SIZE` |
| row_group_size_bytes | unlimited/max | 默认不按字节限制切 row group |
| write_bloom_filter | `true` | 默认启用 bloom filter 写出逻辑 |
| bloom_filter_false_positive_ratio | `0.01` | 默认假阳性率 |
| parquet_version | `V1` | 默认写 V1 编码集合 |
| compression_level | Zstd 默认级别 | 主要影响 ZSTD 等支持 level 的 codec |

#### 8.2 COPY 参数

Parquet copy options：

```cpp
copy_options["row_group_size"] = CopyOption(LogicalType::UBIGINT, CopyOptionMode::READ_WRITE);
copy_options["row_group_size_bytes"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
copy_options["compression"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
copy_options["codec"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
copy_options["compression_level"] = CopyOption(LogicalType::BIGINT, CopyOptionMode::WRITE_ONLY);
copy_options["parquet_version"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
```

读取端也接受 `compression`/`codec`/`row_group_size` 选项，但代码明确说明它们对读取无效，因为读取时这些信息来自文件本身：

```cpp
if (key == "compression" || key == "codec" || key == "row_group_size") {
    // CODEC/COMPRESSION and ROW_GROUP_SIZE options have no effect on parquet read.
    // These options are determined from the file.
    return true;
}
```

#### 8.3 压缩格式兼容性

读取端解压支持在 [column_reader.cpp](/data/workspace/database/duckdb-cuda/extension/parquet/column_reader.cpp)：

```cpp
switch (codec) {
case CompressionCodec::GZIP:
case CompressionCodec::LZ4_RAW:
case CompressionCodec::SNAPPY:
case CompressionCodec::ZSTD:
case CompressionCodec::BROTLI:
    ...
default:
    throw InvalidInputException("Unsupported compression codec ... Supported options are uncompressed, brotli, gzip, lz4_raw, snappy or zstd");
}
```

[extension/parquet/CMakeLists.txt](/data/workspace/database/duckdb-cuda/extension/parquet/CMakeLists.txt) 中包含或链接了 snappy、lz4、brotli、zstd 相关依赖。

**源码可确认事实**：DuckDB 能读取不同 row group 大小的 Parquet，因为 row group 数量和每组 `num_rows` 都来自文件 metadata；`row_group_size` 是写出选项，不约束读取端。

### 9. 关键问题直接回答

#### 9.1 DuckDB 拿到执行计划后碰到 scan operator 如何扫描 Parquet？

`PipelineExecutor` 从 source 拉 `DataChunk`，`PhysicalTableScan` 调用绑定的 table function。对于 Parquet，table function 是 `parquet_scan`，最终进入 `ParquetReader::Scan`，按 row group 切分任务，按 `STANDARD_VECTOR_SIZE` 从 column reader 读取数据并填充 `DataChunk`。

#### 9.2 内存模式或表已经在内存中时如何识别并直接读内存？

不是 Parquet scan 动态识别“这个文件已在内存中”。如果对象是 DuckDB 内部表，执行计划绑定的是 `seq_scan`，它通过 `DuckTableEntry::GetStorage()` 直接扫描 `DataTable`/row groups；如果对象是外部 Parquet，绑定的是 `parquet_scan`，它读 Parquet 文件。两条路径由表对象类型/table function 决定。

#### 9.3 DuckDB 如何进行盘内过滤？

Parquet 使用 footer/row group/column statistics/bloom filter 做 row group pruning；普通表使用 row group/column segment zone map。不能提前排除时，读取过滤列后在内存生成 selection vector，再读取其他列。

#### 9.4 持久化模式下读了一部分表，会形成“残缺表 a”吗？

不会。读入内存的是 scan 输出 chunk、buffer cache、operator state 等，不是语义上的残缺表。后续 query 会重新根据 scan state、filters、projections 扫描所需数据；buffer 命中只是 IO 加速。

#### 9.5 spill 是以表为粒度还是 chunk/block/operator 为粒度？

源码显示 buffer manager 管理 `BlockHandle`/`FileBuffer`/temporary buffer，scan 输出是 `DataChunk`，执行算子可能管理自己的中间状态。因此不应理解为“整表换出”。更准确地说，scan 是流式的，spill/eviction 发生在 buffer/block 或算子中间状态层面。

#### 9.6 数据库大于内存时，持久化模式和内存模式被动溢出哪个更好？

通常持久化模式更好。它天然支持 base table 在磁盘上、按需 scan、buffer cache 加速。内存模式 + spill 更适合临时数据或小热集；临时 spill 不等价于可靠的持久化存储。

#### 9.7 DuckDB 能兼容不同压缩格式、不同 row group 设置的 Parquet 吗？

可以读取多种压缩格式：uncompressed、snappy、gzip、zstd、brotli、lz4_raw。不同 row group 大小由 Parquet metadata 描述，读取端按文件实际 row group 扫描。统计信息缺失时，row group pruning 收益下降，但仍可读取并在内存中过滤。

### 10. 建议的实验验证

以下结论源码可确认，但性能收益建议实验验证：

- 构造带/不带 Parquet statistics 的文件，对比 row group pruning 效果。
- 构造不同 row group size 的 Parquet 文件，对比 scan 并行度和 predicate pushdown 效果。
- 设置较小 `memory_limit` 与 `temp_directory`，运行 hash join/sort/group by，观察临时文件与 profiling。
- 对比 `read_parquet` 直接查询与 `CREATE TABLE AS SELECT * FROM read_parquet(...)` 后 `seq_scan` 的重复查询性能。
