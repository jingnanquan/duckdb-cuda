//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/bitmap_hash_join_executor.hpp
//
// Bitmap-Join (BHJ) executor (design doc §6.4, module M-E).
//
// The BitmapJoinExecutor implements an equi-join between a primary-key
// (dimension) build side and a foreign-key (fact) probe side WITHOUT building a
// JoinHashTable. The build side rowids are recorded in a dense ValidityMask
// (the "bitmap"); the probe side tests a single bit per row. The build side RHS
// output columns are materialized once into per-rowid payload vectors, so the
// probe can reference them as dictionary vectors (zero-copy gather).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalHashJoin;

//! Per-thread build state, held inside HashJoinLocalSinkState. Each build thread
//! accumulates set rowids into its own bitmap with no synchronization
//! (SetValidUnsafe); the thread-local bitmaps are OR-merged in CombineBitmap.
struct BitmapJoinLocalState {
	//! Allocate the thread-local bitmap (all-invalid) lazily. Safe to call repeatedly.
	void Initialize(idx_t bitmap_size);

	//! all-invalid mask sized to the PK table row count; bit i == 1 means build row i passed.
	ValidityMask bitmap;
	bool initialized = false;
};

//! Global Bitmap-Join executor, owned by HashJoinGlobalSinkState. Holds the merged
//! bitmap and the materialized RHS payload columns (one Vector per output column,
//! each sized to the PK row count and indexed by rowid).
class BitmapJoinExecutor {
public:
	explicit BitmapJoinExecutor(const PhysicalHashJoin &join);

	//===------------------------------------------------------------------===//
	// Build
	//===------------------------------------------------------------------===//
	//! Record the build rowids of `chunk` into the thread-local bitmap and scatter
	//! the RHS payload columns into the global payload vectors.
	//! `build_keys` is the result of evaluating the build-side join key expression
	//! (i.e. join_keys.data[0] carries the PK column value).
	void SinkBitmap(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys, BitmapJoinLocalState &lstate);
	//! OR-merge a thread-local bitmap into the global bitmap (called under the sink lock).
	void CombineBitmap(BitmapJoinLocalState &lstate);
	//! Mark the build as ready (no partitioning / pointer table needed).
	void FinalizeBitmap();

	//===------------------------------------------------------------------===//
	// Probe
	//===------------------------------------------------------------------===//
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const;
	OperatorResultType ProbeBitmap(ExecutionContext &context, DataChunk &input, DataChunk &result,
	                                OperatorState &state) const;

	//===------------------------------------------------------------------===//
	// Diagnostics
	//===------------------------------------------------------------------===//
	idx_t BitmapSize() const {
		return bitmap_size;
	}
	bool IsReady() const {
		return ready;
	}

private:
	const PhysicalHashJoin &join;

	//! Number of rows in the PK table => number of bits in the bitmap.
	idx_t bitmap_size = 0;
	//! rowid = pk_value - build_rowid_offset (1-based PK columns reuse rowid_offset == 1).
	int64_t build_rowid_offset = 0;

	//! Union of all build-side rowids (filled during build).
	ValidityMask global_bitmap;
	//! Materialized RHS output columns, each sized to bitmap_size and indexed by rowid.
	vector<unique_ptr<Vector>> payload_columns;
	//! For each RHS output column i, the source column index within the build chunk.
	vector<idx_t> payload_source_columns;

	//! Guards global_bitmap OR-merge and payload scatter (build side is the small side).
	mutex build_lock;
	bool ready = false;
};

} // namespace duckdb
