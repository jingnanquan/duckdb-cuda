#include "duckdb/execution/operator/join/bitmap_hash_join_executor.hpp"

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
	}
}

//===--------------------------------------------------------------------===//
// Build: rowid extraction + payload scatter
//===--------------------------------------------------------------------===//
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
		// 后续通过or合并到全局的bitmap中，这样减少了锁的开销，但是引入的构造和合并开销很难对比。
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
	// 这里使用joinkeys作为rowid直接填入bitmap中
	ExtractBuildRowidsSwitch(build_keys.data[0], count, build_rowid_offset, bitmap_size, lstate.bitmap, valid_rows,
	                         rowids.data(), valid_count);

	if (valid_count == 0 || payload_columns.empty()) {
		return;
	}

	// 2) Scatter the RHS payload columns into the global payload vectors.
	// Build is the small (dimension) side, so serializing the scatter under a lock is cheap
	// and keeps variable-length (string heap) writes safe. (Lock-free scatter is an M4 item.)
	lock_guard<mutex> guard(build_lock);
	for (idx_t c = 0; c < payload_columns.size(); c++) {
		auto &source = chunk.data[payload_source_columns[c]]; //每列Vector单独处理
		ScatterColumn(source, valid_rows, rowids.data(), valid_count, count, *payload_columns[c]);
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

	lock_guard<mutex> guard(build_lock);
	auto *global_data = global_bitmap.GetData();
	const idx_t word_count = ValidityMask::EntryCount(bitmap_size);
	for (idx_t w = 0; w < word_count; w++) {
		global_data[w] |= local_data[w];
	}
}

void BitmapJoinExecutor::FinalizeBitmap() {
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
                               const ValidityMask &bitmap, SelectionVector &probe_sel, SelectionVector &build_sel,
                               idx_t &result_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	idx_t sel = 0;
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
	result_count = sel;
}

static void FillProbeSelectionSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     const ValidityMask &bitmap, SelectionVector &probe_sel,
                                     SelectionVector &build_sel, idx_t &result_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		FillProbeSelection<int8_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::INT16:
		FillProbeSelection<int16_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::INT32:
		FillProbeSelection<int32_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::INT64:
		FillProbeSelection<int64_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::UINT8:
		FillProbeSelection<uint8_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::UINT16:
		FillProbeSelection<uint16_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::UINT32:
		FillProbeSelection<uint32_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
		break;
	case PhysicalType::UINT64:
		FillProbeSelection<uint64_t>(keys, count, offset, bitmap_size, bitmap, probe_sel, build_sel, result_count);
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

	// Resolve the probe-side join key (= rowid + offset) and test the bitmap.
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);
	const idx_t count = state.join_keys.size();

	idx_t result_count = 0;
	FillProbeSelectionSwitch(state.join_keys.data[0], count, build_rowid_offset, bitmap_size, global_bitmap,
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

	result.SetCardinality(result_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
