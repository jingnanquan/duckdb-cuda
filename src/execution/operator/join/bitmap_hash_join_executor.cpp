#include "duckdb/execution/operator/join/bitmap_hash_join_executor.hpp"

#include <atomic>
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BitmapJoinLocalState
//===--------------------------------------------------------------------===//
void BitmapJoinLocalState::Initialize(idx_t bitmap_size) {
	if (initialized) {
		return;
	}
    // ValidityMask类型
	bitmap.Initialize(bitmap_size);
	bitmap.SetAllInvalid(bitmap_size);
	valid_count = 0;
	initialized = true;
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//
BitmapJoinExecutor::BitmapJoinExecutor(const PhysicalHashJoin &join_p) : join(join_p) {
	auto &resolved = join.bitmap_join_resolved;
	if (!resolved.pk) {
		throw InternalException("BitmapJoinExecutor: use_bitmap_join was set but no PK binding was resolved");
	}
	if (!resolved.build_is_pk_side) {
		throw NotImplementedException("BitmapJoinExecutor: only build-is-PK-side joins are supported (MVP §6.4)");
	}
	if (join.join_type != JoinType::INNER) {
		throw NotImplementedException("BitmapJoinExecutor: only INNER joins are supported (MVP §6.4)");
	}
	if (join.conditions.size() != 1) {
		throw NotImplementedException("BitmapJoinExecutor: only a single equi-condition is supported (MVP §6.4)");
	}

	bitmap_size = resolved.pk->row_count;
	build_rowid_offset = resolved.pk->rowid_offset;

	// Global bitmap: all-invalid, sized to the PK row count.
	global_bitmap.Initialize(bitmap_size);
	global_bitmap.SetAllInvalid(bitmap_size);

	// Pre-allocate one payload vector per RHS output column (sized to the PK row count,
	// indexed by rowid) and reconstruct, for each output column, its source column index
	// within the build chunk.
	const idx_t alloc_size = MaxValue<idx_t>(bitmap_size, 1);
	for (idx_t i = 0; i < join.rhs_output_columns.col_idxs.size(); i++) {
		const idx_t out_idx = join.rhs_output_columns.col_idxs[i];
		idx_t source_col;
		if (out_idx < join.condition_types.size()) {
			// This output column is a join key; its build-chunk source is the bound reference
			// of the build-side (right) condition expression.
			auto &right = *join.conditions[out_idx].right;
			if (right.GetExpressionClass() != ExpressionClass::BOUND_REF) {
				throw NotImplementedException(
				    "BitmapJoinExecutor: build-side key output is not a direct column reference (MVP §6.4)");
			}
			source_col = right.Cast<BoundReferenceExpression>().index;
		} else {
			// This output column is a payload column.
			const idx_t payload_pos = out_idx - join.condition_types.size();
			source_col = join.payload_columns.col_idxs[payload_pos];
		}
		payload_source_columns.push_back(source_col);

		auto &col_type = join.rhs_output_columns.col_types[i];
		auto col = make_uniq<Vector>(col_type, alloc_size);   //这里需要分配空间哎，并且分配bitmap_size大小的size，没看懂
		// Pre-initialize validity (all-valid) with capacity == alloc_size so that per-rowid
		// SetInvalid for NULL build values is always in range.
		FlatVector::Validity(*col).Initialize(alloc_size);
		payload_columns.push_back(std::move(col));

		// 条目8: 分流定长/变长 payload 列。定长列的 scatter 写不同 rowid 位置，天然无数据竞争，
		// 可以 lock-free；变长列(VARCHAR)的 scatter 需要操作 StringHeap(ArenaAllocator)，有状态
		// 分配器不能并发，必须加锁。分流后定长列不受 varchar 列的锁阻塞。
		auto internal = col_type.InternalType();
		if (internal == PhysicalType::VARCHAR || internal == PhysicalType::ARRAY) {
			var_payload_indices.push_back(payload_columns.size() - 1);
		} else {
			fixed_payload_indices.push_back(payload_columns.size() - 1);
		}
	}
}

//===--------------------------------------------------------------------===//
// Build: rowid extraction + payload scatter
//===--------------------------------------------------------------------===//
// perf/sf5/combine锁 优化 (§建议1 精简): 之前这里维护了一份独立的 touched_words dirty-bitmap，
// 用于让 CombineBitmap 跳过全 0 的 word。但 "某个 word 是否非零" 本就是 local_bitmap 自身数据
// 的信息 (local_data[w] != 0)，不需要在这里的热路径里再额外做一次分支 + 写入来重复记录同样的
// 信息。CombineBitmap 直接读 local_bitmap 的底层数据判断即可，效果完全等价且更省。
template <class T>
static void ExtractBuildRowids(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                               ValidityMask &local_bitmap, SelectionVector &valid_rows, idx_t *rowids,
                               idx_t &valid_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	idx_t vc = 0;
	for (idx_t i = 0; i < count; i++) {
		const idx_t kidx = kdata.sel->get_index(i);
		if (!kdata.validity.RowIsValid(kidx)) {
			continue;
		}
		// 这里直接使用了全局的offset，也就是每个2048 chunk的数据，实际上都持有了完整长度的local_bitmap
		// 后续通过or合并到全局的bitmap中，这样没有锁的开销，但是引入的构造和合并开销很难对比。
		const int64_t v = static_cast<int64_t>(data[kidx]) - offset; 
		if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
			continue;
		}
		const idx_t rowid = static_cast<idx_t>(v);
		local_bitmap.SetValidUnsafe(rowid);
		valid_rows.set_index(vc, i);  //这里记录了哪些行是有效的。
		rowids[vc] = rowid;    //跟上边构成了一个双重索引，即假定i于rowid不一定相同（rowid有可能从1开始），但vc几乎一定与i相同，两个continue几乎不可能走到
		vc++;
	}
	valid_count = vc;
}

static void ExtractBuildRowidsSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     ValidityMask &local_bitmap, SelectionVector &valid_rows, idx_t *rowids,
                                     idx_t &valid_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		ExtractBuildRowids<int8_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT16:
		ExtractBuildRowids<int16_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT32:
		ExtractBuildRowids<int32_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT64:
		ExtractBuildRowids<int64_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT8:
		ExtractBuildRowids<uint8_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT16:
		ExtractBuildRowids<uint16_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT32:
		ExtractBuildRowids<uint32_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT64:
		ExtractBuildRowids<uint64_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	default:
		throw NotImplementedException("BitmapJoinExecutor: unsupported build key type '%s' (must be integral)",
		                              keys.GetType().ToString());
	}
}

template <class T>
static void ScatterFixed(UnifiedVectorFormat &s, const SelectionVector &valid_rows, const idx_t *rowids, idx_t n,
                         Vector &target) {
	const auto sdata = s.GetData<T>();
	const auto tdata = FlatVector::GetData<T>(target);
	auto &tmask = FlatVector::Validity(target);
	for (idx_t k = 0; k < n; k++) {
		const idx_t i = valid_rows.get_index(k);
		const idx_t sidx = s.sel->get_index(i);
		const idx_t rowid = rowids[k];
		if (!s.validity.RowIsValid(sidx)) {
			tmask.SetInvalid(rowid);
			continue;
		}
		tdata[rowid] = sdata[sidx];
	}
}

//! Scatter source[valid_rows[k]] -> target[rowids[k]] for k in [0, n).
static void ScatterColumn(Vector &source, const SelectionVector &valid_rows, const idx_t *rowids, idx_t n,
                          idx_t src_count, Vector &target) {
	UnifiedVectorFormat s;
	source.ToUnifiedFormat(src_count, s);
	switch (target.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ScatterFixed<int8_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT16:
		ScatterFixed<int16_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT32:
		ScatterFixed<int32_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT64:
		ScatterFixed<int64_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT128:
		ScatterFixed<hugeint_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT8:
		ScatterFixed<uint8_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT16:
		ScatterFixed<uint16_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT32:
		ScatterFixed<uint32_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT64:
		ScatterFixed<uint64_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT128:
		ScatterFixed<uhugeint_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::FLOAT:
		ScatterFixed<float>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::DOUBLE:
		ScatterFixed<double>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::VARCHAR: {
		const auto sdata = s.GetData<string_t>();
		const auto tdata = FlatVector::GetData<string_t>(target);
		auto &tmask = FlatVector::Validity(target);
		for (idx_t k = 0; k < n; k++) {
			const idx_t i = valid_rows.get_index(k);
			const idx_t sidx = s.sel->get_index(i);
			const idx_t rowid = rowids[k];
			if (!s.validity.RowIsValid(sidx)) {
				tmask.SetInvalid(rowid);
				continue;
			}
			tdata[rowid] = StringVector::AddStringOrBlob(target, sdata[sidx]);
		}
		break;
	}
	default: {
		// Correct (but slow) fallback for nested / unsupported physical types.
		auto &tmask = FlatVector::Validity(target);
		for (idx_t k = 0; k < n; k++) {
			const idx_t i = valid_rows.get_index(k);
			const idx_t rowid = rowids[k];
			auto value = source.GetValue(i);
			if (value.IsNull()) {
				tmask.SetInvalid(rowid);
				continue;
			}
			target.SetValue(rowid, value);
		}
		break;
	}
	}
}

void BitmapJoinExecutor::SinkBitmap(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys,
                                    BitmapJoinLocalState &lstate) {
	lstate.Initialize(bitmap_size);

	const idx_t count = chunk.size();
	if (count == 0) {
		return;
	}

	// 1) Extract rowids and set the thread-local bitmap bits (lock-free hot path).
	SelectionVector valid_rows(count);
	vector<idx_t> rowids(count);
	idx_t valid_count = 0;
	// 条目3: when a materialized `_rowid` column was injected into the build chunk (PK column
	// is not itself a dense rowid), read it directly instead of reusing the join key value.
	Vector &rowid_vec = (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX)
	                         ? chunk.data[join.bitmap_build_rowid_idx]
	                         : build_keys.data[0];
	const int64_t rowid_offset =
	    (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX) ? 0 : build_rowid_offset;
	ExtractBuildRowidsSwitch(rowid_vec, count, rowid_offset, bitmap_size, lstate.bitmap, valid_rows, rowids.data(),
	                         valid_count);
	// perf/sf5/combine锁 优化 (§建议2): 本线程累计有效 build 行数，供 CombineBitmap 汇总到
	// total_valid_count，FinalizeBitmap 借此以 O(线程数) 求和替代 CountValid 全量扫描。
	lstate.valid_count += valid_count;

	if (valid_count == 0 || payload_columns.empty()) {
		return;
	}

	// 2) Scatter the RHS payload columns into the global payload vectors.
	// 条目8: 分流处理 - 定长列 lock-free（写不同 rowid 位置，无数据竞争），
	// 变长列(VARCHAR) 加锁（StringHeap 的 ArenaAllocator 有状态，不能并发）。
	// 这样 Q9 的全定长 join（如 DATE/BIGINT）完全无锁，Q5 的混合 join 中
	// 定长列也不被 varchar 列的锁阻塞。
	for (idx_t c : fixed_payload_indices) {
		auto &source = chunk.data[payload_source_columns[c]];
		ScatterColumn(source, valid_rows, rowids.data(), valid_count, count, *payload_columns[c]);
	}
	if (!var_payload_indices.empty()) {
		lock_guard<mutex> guard(build_lock);
		for (idx_t c : var_payload_indices) {
			auto &source = chunk.data[payload_source_columns[c]];
			ScatterColumn(source, valid_rows, rowids.data(), valid_count, count, *payload_columns[c]);
		}
	}
}

void BitmapJoinExecutor::CombineBitmap(BitmapJoinLocalState &lstate) {
	if (!lstate.initialized) {
		return; // this thread saw no build data
	}
	auto *local_data = lstate.bitmap.GetData();
	if (!local_data) {
		return; // all-invalid: nothing to OR
	}

	// 条目8 (CombineBitmap优化): 方案1(atomic fetch_or) + 方案2(稀疏合并)
	//
	// 方案1: 用 std::atomic<validity_t>::fetch_or 替代 mutex，实现无锁合并。
	//   - validity_t = uint64_t，全局 bitmap 数组 reinterpret_cast 为 atomic<uint64_t>*
	//   - fetch_or 是硬件级原子操作（x86 LOCK OR 指令），比 mutex 快几个数量级
	//   - OR 操作可交换可结合，多线程并发 fetch_or 结果正确
	//
	// 方案2: 只遍历本线程实际非 0 的 word，跳过全 0 的 word。
	//   - 低密度场景（如 Q5 密度3%），97% 的 word 是 0，跳过它们大幅减少 cache miss
	//   - perf/sf5/combine锁 优化 (§建议1 精简): 之前用一份独立的 touched_words dirty-bitmap
	//     记录 "哪些 word 被 touch 过"，但这本就是 local_data[w] 自身能直接回答的信息
	//     (word 非 0 <=> 被 touch 过)，无需在 SinkBitmap 热路径里重复维护，这里直接读
	//     local_data[w] 判断即可，效果完全等价且省掉了一份数组的分配/清零/写入开销。
	// lock_guard<mutex> guard(build_lock);
	// auto *global_data = global_bitmap.GetData();
	// const idx_t word_count = ValidityMask::EntryCount(bitmap_size);
	// for (idx_t w = 0; w < word_count; w++) {
	// 	global_data[w] |= local_data[w];
	// }

	auto *global_data = global_bitmap.GetData();
	auto *atomic_global = reinterpret_cast<std::atomic<validity_t> *>(global_data);
	D_ASSERT(atomic_global);

	const idx_t word_count = ValidityMask::EntryCount(bitmap_size);
	for (idx_t w = 0; w < word_count; w++) {
		if (local_data[w] != 0) {
			atomic_global[w].fetch_or(local_data[w], std::memory_order_relaxed);
		}
	}

	// perf/sf5/combine锁 优化 (§建议2): 汇总本线程累计的有效 build 行数到全局计数，
	// 供 FinalizeBitmap 以 O(1) 读取替代 O(bitmap_size/64) 的 CountValid 全量扫描。
	total_valid_count.fetch_add(lstate.valid_count, std::memory_order_relaxed);
}

void BitmapJoinExecutor::FinalizeBitmap() {
	// 5.1 (perf/sf5/combine锁 / 根因分析-详细版.md §5.1): 稠密 fast-path。
	// bitmap 全部置位 (popcount == bitmap_size) 时, probe 可跳过逐行位测试。
	//
	// perf/sf5/combine锁 优化 (§建议2): 不再对 global_bitmap 做一次全量 CountValid 扫描
	// (O(bitmap_size/64) 次 popcount)，而是直接用 CombineBitmap 阶段各线程原子累加好的
	// total_valid_count。BHJ 的前提是 build 侧为 PK(唯一)侧，同一 rowid 不会被两个线程
	// 重复置位，因此 "各线程有效行数之和 == bitmap_size" 与 "global_bitmap popcount ==
	// bitmap_size" 严格等价；即便该唯一性前提出现意外违反，也只会让本判断偏保守 (少判
	// dense)，不会误判 dense 造成越界/漏检，不引入正确性风险。
	is_build_dense = (total_valid_count.load(std::memory_order_relaxed) == bitmap_size);
	ready = true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class BitmapJoinOperatorState : public CachingOperatorState {
public:
	BitmapJoinOperatorState(ClientContext &context, const PhysicalHashJoin &join) : probe_executor(context) {
		auto &allocator = Allocator::Get(context);
		if (!join.lhs_output_columns.col_types.empty()) {
			lhs_output.Initialize(allocator, join.lhs_output_columns.col_types);
		}
		join_keys.Initialize(allocator, join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk lhs_output;
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector probe_sel_vec; // matched probe row indices
	SelectionVector build_sel_vec; // matched build rowids (dictionary indices)
};

unique_ptr<OperatorState> BitmapJoinExecutor::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<BitmapJoinOperatorState>(context.client, join);
}

template <class T>
static void FillProbeSelection(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                               const ValidityMask &bitmap, bool is_build_dense, SelectionVector &probe_sel,
                               SelectionVector &build_sel, idx_t &result_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	idx_t sel = 0;
	if (is_build_dense) {
		// 5.1 稠密 fast-path: bitmap 全置位, 命中即 rowid, 不访问 bitmap, 无位测试分支。
		// 仅保留 NULL 与越界 (v<0 / v>=bitmap_size) 两个必需的正确性检查, 内部不含 bitmap 位测试。
		for (idx_t i = 0; i < count; i++) {
			const idx_t kidx = kdata.sel->get_index(i);
			if (!kdata.validity.RowIsValid(kidx)) {
				continue;
			}
			const int64_t v = static_cast<int64_t>(data[kidx]) - offset;
			if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
				continue;
			}
			const idx_t rowid = static_cast<idx_t>(v);
			probe_sel.set_index(sel, i);
			build_sel.set_index(sel, rowid);
			sel++;
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			const idx_t kidx = kdata.sel->get_index(i);
			if (!kdata.validity.RowIsValid(kidx)) {
				continue;
			}
			const int64_t v = static_cast<int64_t>(data[kidx]) - offset;
			if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
				continue;
			}
			const idx_t rowid = static_cast<idx_t>(v);
			if (bitmap.RowIsValidUnsafe(rowid)) {
				probe_sel.set_index(sel, i);
				build_sel.set_index(sel, rowid);
				sel++;
			}
		}
	}
	result_count = sel;
}

static void FillProbeSelectionSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     const ValidityMask &bitmap, bool is_build_dense, SelectionVector &probe_sel,
                                     SelectionVector &build_sel, idx_t &result_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		FillProbeSelection<int8_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                           result_count);
		break;
	case PhysicalType::INT16:
		FillProbeSelection<int16_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                            result_count);
		break;
	case PhysicalType::INT32:
		FillProbeSelection<int32_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                            result_count);
		break;
	case PhysicalType::INT64:
		FillProbeSelection<int64_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                            result_count);
		break;
	case PhysicalType::UINT8:
		FillProbeSelection<uint8_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                            result_count);
		break;
	case PhysicalType::UINT16:
		FillProbeSelection<uint16_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                             result_count);
		break;
	case PhysicalType::UINT32:
		FillProbeSelection<uint32_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                             result_count);
		break;
	case PhysicalType::UINT64:
		FillProbeSelection<uint64_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, probe_sel, build_sel,
		                             result_count);
		break;
	default:
		throw NotImplementedException("BitmapJoinExecutor: unsupported probe key type '%s' (must be integral)",
		                              keys.GetType().ToString());
	}
}

OperatorResultType BitmapJoinExecutor::ProbeBitmap(ExecutionContext &context, DataChunk &input, DataChunk &result,
                                                   OperatorState &state_p) const {
	auto &state = state_p.Cast<BitmapJoinOperatorState>();

	// LHS output columns (zero-copy reference into the probe chunk).
	state.lhs_output.ReferenceColumns(input, join.lhs_output_columns.col_idxs);
	// 修复 probe 结果组装时的 SelectionVector::Slice 越界 (perf/sf5/combine锁 段错误):
	// 上游算子可能产生 DICTIONARY 类型的 lhs 列, 当它的 selection count 小于本批 probe
	// 行数时, result.Slice(lhs_output_dict, probe_sel, ...) 会做字典链式切片并越界读。
	// 这里 flatten 成 FLAT 向量, 后续切片都是干净的字典, 不会越界。
	state.lhs_output.Flatten();

	// 条目3: when a materialized `*_ref` column was injected into the probe chunk, read it
	// directly and skip evaluating the join-key expression entirely.
	idx_t count;
	Vector *ref_vec;
	int64_t ref_offset;
	if (join.bitmap_probe_ref_idx != DConstants::INVALID_INDEX) {
		count = input.size();
		ref_vec = &input.data[join.bitmap_probe_ref_idx];
		ref_offset = 0;
	} else {
		// Fast path: resolve the probe-side join key (= rowid + offset) and test the bitmap.
		state.join_keys.Reset();
		state.probe_executor.Execute(input, state.join_keys);
		count = state.join_keys.size();
		ref_vec = &state.join_keys.data[0];
		ref_offset = build_rowid_offset;
	}

	idx_t result_count = 0;
	FillProbeSelectionSwitch(*ref_vec, count, ref_offset, bitmap_size, global_bitmap, is_build_dense,
	                         state.probe_sel_vec, state.build_sel_vec, result_count);

	// LHS columns: reference directly when every row matched (inner join), else slice.
	if (result_count == count) {
		result.Reference(state.lhs_output);
	} else {
		result.Slice(state.lhs_output, state.probe_sel_vec, result_count, 0);
	}

	// RHS columns: dictionary-reference the materialized payload vectors by rowid.
	const idx_t lhs_count = state.lhs_output.ColumnCount();
	for (idx_t i = 0; i < payload_columns.size(); i++) {
		result.data[lhs_count + i].Slice(*payload_columns[i], state.build_sel_vec, result_count);
	}

	// 条目8 (b_idea/6.4遗漏问题 任务2): append passthrough columns from the probe input after
	// the normal [lhs_output][rhs_payload] columns. These are hidden columns that bypass the
	// intermediate join's [left][right] layout. They come from the probe (LHS) input and must
	// be sliced to match the matched rows (state.probe_sel_vec).
	if (!join.passthrough_lhs_col_idxs.empty() && result_count > 0) {
		idx_t passthrough_start = lhs_count + payload_columns.size();
		for (idx_t i = 0; i < join.passthrough_lhs_col_idxs.size(); i++) {
			result.data[passthrough_start + i].Slice(
			    input.data[join.passthrough_lhs_col_idxs[i]], state.probe_sel_vec, result_count);
		}
	}

	result.SetCardinality(result_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
