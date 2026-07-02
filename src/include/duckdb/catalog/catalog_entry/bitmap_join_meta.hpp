//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/bitmap_join_meta.hpp
//
// Bitmap-Join (BHJ) catalog metadata registry (design doc §6.2, module M-C).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

//! Describes a primary-key (dimension) table whose rows are addressable by a dense rowid.
struct BitmapJoinPKBinding {
	string pk_table;
	string pk_column;    //! original PK column
	string rowid_column; //! column that actually carries the rowid role (may == pk_column)
	int64_t rowid_offset = 0; //! value such that rowid = pk_value - rowid_offset (when reusing the PK column)
	idx_t row_count = 0;      //! number of rows in the PK table (determines bitmap size)
};

//! Describes a foreign-key (fact) column that has a materialized *_ref column pointing at PK rowids.
struct BitmapJoinFKBinding {
	string fk_table;
	string fk_column;  //! original FK column
	string ref_column; //! materialized *_ref column (0-based rowid into the PK table)
	string pk_table;
	string pk_column;
};

//! Result of resolving a single equi-join condition against the registry.
struct BitmapJoinResolved {
	const BitmapJoinPKBinding *pk = nullptr; //! build (dimension) side
	const BitmapJoinFKBinding *fk = nullptr; //! probe (fact) side
	//! true  -> the PK table is on the build side (RHS), FK table is on the probe side (LHS)
	//! false -> reversed
	bool build_is_pk_side = true;
};

//! Process-wide registry of bitmap-join bindings.
//!
//! The registry is a process-level singleton (design decision: §6.2.3 / Q2-A). It is populated
//! either offline via LoadFromJson (parquet extension load hook) or directly in tests via
//! RegisterPK / RegisterFK. All accessors are thread-safe.
class BitmapJoinMetaRegistry {
public:
	//! Returns the process-wide singleton. The ClientContext argument is accepted to match the
	//! design signature but is currently unused (the registry is not scoped to a database instance).
	DUCKDB_API static BitmapJoinMetaRegistry &Get(ClientContext &context);
	//! Context-free accessor (used by tests and code paths without a ClientContext).
	DUCKDB_API static BitmapJoinMetaRegistry &GetInstance();

	//! Load (and merge) bindings from a bitmap_join_meta.json file produced by add_bitmap_columns.py.
	DUCKDB_API void LoadFromJson(const string &path);
	//! Load (and merge) bindings from an in-memory JSON string.
	DUCKDB_API void LoadFromJsonString(const string &json_text);

	//! Manual registration (used by tests).
	DUCKDB_API void RegisterPK(BitmapJoinPKBinding binding);
	DUCKDB_API void RegisterFK(BitmapJoinFKBinding binding);

	//! Remove all bindings.
	DUCKDB_API void Clear();

	DUCKDB_API optional_ptr<const BitmapJoinPKBinding> FindPK(const string &pk_table, const string &pk_column) const;
	DUCKDB_API optional_ptr<const BitmapJoinFKBinding> FindFK(const string &fk_table, const string &fk_column) const;

	//! Given an equi-join condition (build_table.build_col = probe_table.probe_col), determine whether
	//! it is pre-bound (PK <-> FK). On success, fills `out` and returns true.
	DUCKDB_API bool TryResolve(const string &build_table, const string &build_col, const string &probe_table,
	                           const string &probe_col, BitmapJoinResolved &out) const;

	//! Snapshot accessors for the diagnostic PRAGMA.
	DUCKDB_API vector<BitmapJoinPKBinding> GetPKBindings() const;
	DUCKDB_API vector<BitmapJoinFKBinding> GetFKBindings() const;

	//! Global "force bitmap join" switch (design §6.3 hook enablement). Default false so that the
	//! default execution path is never affected.
	DUCKDB_API void SetForceBitmapJoin(bool enabled);
	DUCKDB_API bool IsForceBitmapJoin() const;

	//===------------------------------------------------------------------===//
	// Test-only resolution override
	//===------------------------------------------------------------------===//
	//! For tests / forced execution: set a single PK binding to use whenever the planner needs
	//! to fill in a `BitmapJoinResolved` but column-binding -> catalog reverse-resolution is not
	//! available yet (module 6.6 is the proper resolver). When the override is set and the global
	//! force switch is true, every BHJ-eligible PhysicalHashJoin is wired to this PK binding.
	//! Pass an empty table/column to clear the override.
	DUCKDB_API void SetForceResolvedPK(const string &pk_table, const string &pk_column);
	//! Returns the currently configured override PK binding, or nullptr if none is set.
	DUCKDB_API const BitmapJoinPKBinding *GetForceResolvedPK() const;

private:
	BitmapJoinMetaRegistry() = default;

	//! Internal, assumes the lock is already held.
	const BitmapJoinPKBinding *FindPKUnsafe(const string &pk_table, const string &pk_column) const;
	const BitmapJoinFKBinding *FindFKUnsafe(const string &fk_table, const string &fk_column) const;

	mutable mutex lock;
	vector<BitmapJoinPKBinding> pk_bindings;
	vector<BitmapJoinFKBinding> fk_bindings;
	bool force_bitmap_join = false;
	//! Optional override: when non-empty, points at the PK binding to use for forced BHJ execution.
	string force_pk_table;
	string force_pk_column;
};

} // namespace duckdb
