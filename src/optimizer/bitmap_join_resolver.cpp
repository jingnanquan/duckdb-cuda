#include "duckdb/optimizer/bitmap_join_resolver.hpp"

#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

//! Which side of an intermediate LOGICAL_COMPARISON_JOIN a traced binding came from. Only
//! meaningful for PathStep entries whose `op` is a join (see TraceBindingToGet / 条目4).
enum class PropagationSide { LEFT, RIGHT };

//! One hop on the way from a hidden column's ultimate source LogicalGet back up to the join
//! that needs to consume it. `side` is only meaningful when `op` is a LOGICAL_COMPARISON_JOIN.
struct PathStep {
	reference<LogicalOperator> op;
	PropagationSide side = PropagationSide::LEFT;

	explicit PathStep(LogicalOperator &op_p) : op(op_p) {
	}
	PathStep(LogicalOperator &op_p, PropagationSide side_p) : op(op_p), side(side_p) {
	}
};

//! Result of tracing a join condition's ColumnBinding down to its ultimate source LogicalGet,
//! possibly unwrapping a chain of LOGICAL_FILTER / LOGICAL_PROJECTION / (INNER) LOGICAL_
//! COMPARISON_JOIN operators along the way (design doc §3.2 Step F, §4.3 Step A). `path` holds
//! those intermediate operators in top-down order (path.front() is nearest the join child,
//! path.back() is nearest the Get) - PropagateHiddenColumn (see below) walks it in reverse to
//! inject/propagate a hidden column back up to the same point `get` was reached from.
struct TracedBinding {
	optional_ptr<LogicalGet> get;
	idx_t get_column_index = DConstants::INVALID_INDEX; //! position within get->GetColumnIds()
	vector<PathStep> path;
	//! False if the walk down to `get` passed through an operator type PropagateHiddenColumn
	//! doesn't know how to safely re-expose a new column through (e.g. an Aggregate, or a join
	//! whose type doesn't guarantee a plain "left ++ right" output - see 条目4 §4.3 Step A).
	//! `get`/`get_column_index` remain valid and safe to use for catalog-name resolution (条目
	//! 1/2) regardless of this flag - it only gates whether hidden-column propagation (条目3/4)
	//! may be attempted using `path`.
	bool path_complete = true;
};

//! Returns the ColumnBinding of `expr` if it is (still) a plain BoundColumnRefExpression, or
//! nullptr otherwise (e.g. the condition side is a computed expression such as a cast).
optional_ptr<const ColumnBinding> GetPlainColumnBinding(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &expr.Cast<BoundColumnRefExpression>().binding;
}

//! Walks down from `op`, following `binding` through at most a chain of LOGICAL_PROJECTION
//! (only if the referenced expression is itself a plain column reference - anything else, e.g. a
//! computed expression, is out of scope and handled by falling back to a regular hash join
//! instead), LOGICAL_FILTER (transparent pass-through, optionally reordered/subset via
//! projection_map) and *plain INNER* LOGICAL_COMPARISON_JOIN (条目4: an intermediate join in a
//! multi-join chain - transparent "left ++ right" pass-through; see the Step A comment below for
//! why only INNER is safe) operators, until it reaches the LogicalGet that produces `binding`.
//! Any other operator type falls back to a plain DFS over its children (this preserves the
//! original, pre-条目3, "just find a LogicalGet with a matching table_index" behaviour for
//! anything not on the supported chain) - the Get is still found (for 条目1/2's catalog-name
//! resolution), but `path_complete` is set to false so hidden-column propagation is never
//! attempted across whatever we didn't understand.
TracedBinding TraceBindingToGet(LogicalOperator &op, ColumnBinding binding) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_index != binding.table_index) {
			return TracedBinding();
		}
		TracedBinding result;
		result.get = &get;
		result.get_column_index = binding.column_index;
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op.Cast<LogicalProjection>();
		if (proj.table_index == binding.table_index) {
			if (binding.column_index >= proj.expressions.size()) {
				return TracedBinding();
			}
			auto plain = GetPlainColumnBinding(*proj.expressions[binding.column_index]);
			if (!plain) {
				// Not a plain pass-through column (e.g. a computed expression) - can't trace
				// further back with our scope. Safe fallback: skip BHJ for this join.
				return TracedBinding();
			}
			auto result = TraceBindingToGet(*op.children[0], *plain);
			if (result.get) {
				result.path.emplace(result.path.begin(), op);
			}
			return result;
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		// LogicalFilter has no table_index of its own: LogicalOperator::MapBindings sets
		// result[i] = child_bindings[projection_map[i]] - i.e. `projection_map` only selects
		// *which* child bindings are exposed and in what order, it never rewrites the
		// ColumnBinding *values* themselves. So `binding` (already known to be one of the
		// Filter's own exposed bindings) is, value-for-value, already a valid binding of the
		// child too - no translation needed, just recurse unchanged.
		auto result = TraceBindingToGet(*op.children[0], binding);
		if (result.get) {
			result.path.emplace(result.path.begin(), op);
		}
		return result;
	} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// 条目4 Step A: LogicalJoin::GetColumnBindings() = MapBindings(left, left_projection_map)
		// [++ MapBindings(right, right_projection_map)] - a value-preserving selection/reorder of
		// each child's own bindings, exactly like LogicalFilter's projection_map. Which side(s)
		// are actually exposed in the concatenation - and in what order - depends on join_type
		// (SEMI/ANTI expose only the left side, RIGHT_SEMI/RIGHT_ANTI only the right, MARK adds an
		// extra column, etc.). Only plain INNER joins guarantee the simple, uniform "left's own
		// bindings, then right's own bindings, nothing dropped, nothing added" shape - matching
		// why RemoveUnusedColumns itself only ever rewrites column references for INNER joins
		// (see remove_unused_columns.cpp: `if (comp_join.join_type != JoinType::INNER) break;`).
		//
		// Even for INNER joins, though, propagating (i.e. *growing*) through the LEFT side is
		// unsafe and NOT supported: the join's own combined output is always laid out as
		// [left's bindings][right's bindings], in that fixed order (LogicalJoin::GetColumnBindings
		// concatenates, never reorders across sides). Some ancestor further up the plan may
		// already reference this join's RIGHT-side contributions via *absolute* positions baked
		// in by an earlier optimizer pass (e.g. its own left/right_projection_map, computed once
		// by RemoveUnusedColumns long before we run) - growing the LEFT side inserts a new entry
		// *before* those positions and silently shifts all of them, corrupting any such
		// unrelated ancestor reference (this is exactly what caused a real, reproduced crash on
		// TPCH Q5's `orders JOIN lineitem` chain during development). Growing the RIGHT side is
		// always safe, since nothing in this join's own output layout follows it. So: only the
		// RIGHT side is ever marked as a supported propagation hop; a LEFT-side match still finds
		// the underlying Get (for 条目1/2's catalog-name-only purposes) but is downgraded to
		// path_complete=false, so PropagateHiddenColumn is never attempted across it - the
		// consuming join safely falls back to a regular hash join instead.
		bool join_is_inner = op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER;
		auto left_result = TraceBindingToGet(*op.children[0], binding);
		if (left_result.get) {
			// LEFT side: never safe to grow - see comment above.
			left_result.path_complete = false;
			return left_result;
		}
		auto right_result = TraceBindingToGet(*op.children[1], binding);
		if (right_result.get) {
			if (join_is_inner) {
				right_result.path.emplace(right_result.path.begin(), op, PropagationSide::RIGHT);
			} else {
				right_result.path_complete = false;
			}
		}
		return right_result;
	}
	// Generic fallback: recurse into children without unwrapping `binding` (covers e.g. nested
	// Aggregates, ASOF/DELIM joins, or other operators that don't affect the binding's
	// table_index - matches the original FindSourceGet DFS semantics). No path is recorded
	// through these operators, and path_complete is downgraded to false: propagation only
	// supports a Filter/Projection/plain-INNER-Join chain, not arbitrary operators.
	for (auto &child : op.children) {
		auto result = TraceBindingToGet(*child, binding);
		if (result.get) {
			result.path_complete = false;
			return result;
		}
	}
	return TracedBinding();
}

//! Resolve the "logical table name" for a LogicalGet: the catalog table name for real tables,
//! or (falling back) the parquet file's base name (no directory, no extension) for
//! read_parquet(...)-style table functions. This matches the naming convention used by
//! add_bitmap_columns.py when it writes pk_table/fk_table into bitmap_join_meta.json (design
//! doc §1.3 Step B). Known limitation: aliases are not considered (they don't survive past the
//! binder) - see design doc R-NAME. Returns an empty string if neither source is available.
string ResolveLogicalTableName(LogicalGet &get) {
	auto table = get.GetTable();
	if (table) {
		return table->name;
	}
	auto *multi_file_data = dynamic_cast<MultiFileBindData *>(get.bind_data.get());
	if (multi_file_data && multi_file_data->file_list) {
		auto first_file = multi_file_data->file_list->GetFirstFile();
		if (!first_file.path.empty()) {
			auto fs = FileSystem::CreateLocal();
			return fs->ExtractBaseName(first_file.path);
		}
	}
	return string();
}

//! `column_index` here is a ColumnBinding::column_index for a LogicalGet, i.e. the position of
//! the column within GetColumnIds() - NOT the physical column index of the underlying table
//! (see design doc §1.3 "陷阱2"). Returns an empty string if out of range.
string ResolveColumnName(LogicalGet &get, idx_t column_index) {
	auto &column_ids = get.GetColumnIds();
	if (column_index >= column_ids.size()) {
		return string();
	}
	return get.GetColumnName(column_ids[column_index]);
}

//! Finds the physical column id of `column_name` on `get` (P0 principle: this is the one place
//! that translates a catalog column name into a physical column_t; everything else works purely
//! off ColumnBinding/column_t), then ensures it is part of the scan's output - reusing an
//! already-scanned occurrence if present (idempotency, design doc §3.3 test 3), or forcing it in
//! via AddColumnId otherwise. Returns its position within get.GetColumnIds().
idx_t EnsureGetColumn(LogicalGet &get, const string &column_name) {
	idx_t physical_id = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < get.names.size(); i++) {
		if (get.names[i] == column_name) {
			physical_id = i;
			break;
		}
	}
	if (physical_id == DConstants::INVALID_INDEX) {
		throw InternalException("BitmapJoinResolver: hidden column \"%s\" not found on table scan \"%s\"",
		                        column_name, ResolveLogicalTableName(get));
	}
	auto &column_ids = get.GetColumnIds();
	idx_t column_ids_pos = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto &col = column_ids[i];
		if (!col.HasChildren() && col.HasPrimaryIndex() && col.GetPrimaryIndex() == physical_id) {
			// Already scanned (e.g. it happens to also be the plain join key, or a previous
			// resolver pass over the same plan already injected it) - reuse its position.
			column_ids_pos = i;
			break;
		}
	}
	if (column_ids_pos == DConstants::INVALID_INDEX) {
		get.AddColumnId(physical_id);
		column_ids_pos = get.GetColumnIds().size() - 1;
	}
	// Regular table scans set `function.filter_prune == true`, which makes RemoveUnusedColumns
	// additionally gate the Get's *visible* output through `projection_ids` on top of
	// `column_ids` (a column can be scanned - e.g. for a pushed-down filter - without being part
	// of the operator's output). `GetColumnBindings()`/`types` follow `projection_ids`, not
	// `column_ids`, whenever the former is non-empty (see LogicalGet::GetColumnBindings /
	// ResolveTypes) - AddColumnId alone does NOT make a column visible in that case. Make sure
	// our column is actually exposed.
	if (!get.projection_ids.empty()) {
		bool already_visible = false;
		for (auto proj_id : get.projection_ids) {
			if (proj_id == column_ids_pos) {
				already_visible = true;
				break;
			}
		}
		if (!already_visible) {
			get.projection_ids.push_back(column_ids_pos);
			if (!get.types.empty()) {
				get.types.push_back(get.GetColumnType(get.GetColumnIds()[column_ids_pos]));
			}
		}
	} else if (!get.types.empty() && get.types.size() < get.GetColumnIds().size()) {
		// projection_ids empty -> types tracks column_ids 1:1; we just grew column_ids.
		get.types.push_back(get.GetColumnType(get.GetColumnIds()[column_ids_pos]));
	}
	return column_ids_pos;
}

//! Position of `column_ids_pos` within `get`'s own current output bindings list, i.e. as
//! returned by `get.GetColumnBindings()` - identical to `column_ids_pos` when `projection_ids`
//! is empty, otherwise its position *within* `projection_ids` (LogicalGet::GetColumnBindings
//! emits one binding per `projection_ids` entry, using that entry's *value* - not its position -
//! as the ColumnBinding's column_index). Needed because LogicalFilter::projection_map entries
//! are positions into the child's bindings list, not raw ColumnBinding::column_index values.
idx_t GetOutputPosition(LogicalGet &get, idx_t column_ids_pos) {
	if (get.projection_ids.empty()) {
		return column_ids_pos;
	}
	for (idx_t i = 0; i < get.projection_ids.size(); i++) {
		if (get.projection_ids[i] == column_ids_pos) {
			return i;
		}
	}
	throw InternalException("BitmapJoinResolver: hidden column missing from projection_ids after EnsureGetColumn");
}

//! Returns the position of `value` within `map`, appending it via push_back first if not
//! already present. LogicalFilter::projection_map / LogicalJoin::left_projection_map /
//! right_projection_map entries are positions into the child's own bindings list - if our hidden
//! column's position is *already* exposed there (routine for a `SELECT *`-style query, where a
//! real, user-visible column happens to sit at the exact position we need), blindly push_back-ing
//! another copy would silently duplicate that binding in the operator's own output, corrupting
//! every subsequent position-based computation both here and in ColumnBindingResolver. Mirrors
//! EnsureGetColumn's identical dedup logic one level up (LogicalGet::column_ids/projection_ids).
idx_t FindOrAppend(vector<idx_t> &map, idx_t value) {
	for (idx_t i = 0; i < map.size(); i++) {
		if (map[i] == value) {
			return i;
		}
	}
	map.push_back(value);
	return map.size() - 1;
}

//! Injects (or reuses) a hidden physical column named `column_name` into `get`'s scan output,
//! then propagates it up along `path` (as returned by TraceBindingToGet, top-down order) through
//! any LOGICAL_PROJECTION (append a new expression referencing it), LOGICAL_FILTER (extend
//! projection_map, if present), or plain-INNER LOGICAL_COMPARISON_JOIN (条目4: extend
//! left_projection_map/right_projection_map, if present; otherwise the column already flows
//! through automatically since the join's output is an untouched "left ++ right" concatenation -
//! see TraceBindingToGet's LOGICAL_COMPARISON_JOIN branch for why this is only attempted for
//! INNER joins) operators - mirroring LateMaterialization::ConstructRHS. Returns a
//! BoundColumnRefExpression with the resulting binding, valid as seen from the top of `path`
//! (i.e. from the join's own child on this side) - ColumnBindingResolver will flatten it into a
//! BoundReferenceExpression with the correct physical chunk offset once
//! bhj_build_rowid_ref/bhj_probe_ref_ref are visited (see column_binding_resolver.cpp).
unique_ptr<Expression> PropagateHiddenColumn(LogicalGet &get, const vector<PathStep> &path,
                                             const string &column_name) {
	idx_t column_index = EnsureGetColumn(get, column_name);
	auto &column_id = get.GetColumnIds()[column_index];
	auto &column_type = get.GetColumnType(column_id);
	ColumnBinding binding(get.table_index, column_index);
	//! Position of the hidden column within whatever operator directly below the one currently
	//! being processed in the loop outputs - the index to use if a LOGICAL_FILTER/plain-INNER-
	//! join above needs to extend its projection_map (those entries are *positions* into the
	//! child's bindings list, per LogicalOperator::MapBindings - not raw column_index values).
	idx_t child_binding_pos = GetOutputPosition(get, column_index);

	for (idx_t i = path.size(); i > 0; i--) {
		auto &step = path[i - 1];
		auto &cur = step.op.get();
		if (cur.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = cur.Cast<LogicalProjection>();
			proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(column_name, column_type, binding));
			binding = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			child_binding_pos = proj.expressions.size() - 1;
		} else if (cur.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = cur.Cast<LogicalFilter>();
			if (filter.HasProjectionMap()) {
				child_binding_pos = FindOrAppend(filter.projection_map, child_binding_pos);
			}
			// else: filter is a transparent pass-through - `binding` / `child_binding_pos`
			// carry over unchanged to whatever operator comes next up the path.
		} else {
			// 条目4: intermediate plain-INNER LOGICAL_COMPARISON_JOIN, always reached via its
			// RIGHT side (see TraceBindingToGet - growing the LEFT side is never recorded as a
			// supported hop, since it would shift the absolute positions of everything in the
			// RIGHT side, corrupting any unrelated ancestor that already references this join's
			// RIGHT-side output by absolute position). Growing right_projection_map itself is
			// always safe: nothing in this join's own output layout follows the RIGHT part.
			D_ASSERT(cur.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
			D_ASSERT(step.side == PropagationSide::RIGHT);
			auto &join = cur.Cast<LogicalComparisonJoin>();
			if (!join.right_projection_map.empty()) {
				child_binding_pos = FindOrAppend(join.right_projection_map, child_binding_pos);
			}
			// else: identity passthrough on the right side - child_binding_pos (position within
			// the right child's own output) is already correct once offset by the left width.
			idx_t left_len = join.left_projection_map.empty() ? join.children[0]->GetColumnBindings().size()
			                                                   : join.left_projection_map.size();
			child_binding_pos += left_len;
			// binding VALUE is untouched: LogicalJoin::GetColumnBindings selects/reorders via
			// MapBindings but never retags with its own table_index (unlike LogicalProjection).
		}
	}
	// IMPORTANT: every operator we just touched (Projection.expressions / Filter.projection_map /
	// Join.*_projection_map) has a *cached* `.types` field (LogicalOperator::types) that mirrors
	// GetColumnBindings() but is NOT automatically kept in sync when those lists are mutated
	// directly (each ResolveTypes() override just recomputes from scratch, on-demand, the one
	// time it's normally called during binding/optimization - see LogicalProjection::ResolveTypes
	// / LogicalFilter::ResolveTypes / LogicalJoin::ResolveTypes). A stale `.types` doesn't break
	// *this* propagation (we never read `.types` above, only GetColumnBindings()), but
	// ColumnBindingResolver's own general-case handling (`bindings = op.GetColumnBindings(); types
	// = op.types;`) does read it - a size mismatch there silently corrupts whatever *other*,
	// unrelated column reference happens to be resolved against this same operator afterwards
	// (its correct .size()-based type/position bookkeeping assumes `types` == the current, fresh
	// GetColumnBindings()). ResolveOperatorTypes() recomputes `.types` bottom-up for `node` and
	// everything below it from the *current* (just-mutated) state, in one correct, robust sweep -
	// far simpler and safer than trying to patch each `.types` vector incrementally in lockstep
	// with every FindOrAppend/push_back above. Only call it on `path.front()` (nearest the
	// consuming join, i.e. covering every mutated node once) or `get` itself if no propagation was
	// needed - never on `get` when `path` is non-empty, since path.front()'s recursion already
	// covers `get` too and calling it twice would just be redundant (not incorrect, just wasted
	// work).
	auto &root = path.empty() ? static_cast<LogicalOperator &>(get) : path.front().op.get();
	root.ResolveOperatorTypes();
	return make_uniq<BoundColumnRefExpression>(column_name, column_type, binding);
}

} // namespace

void BitmapJoinResolver::VisitOperator(LogicalOperator &op) {
	// Recurse first so nested joins get their own hint independently.
	VisitOperatorChildren(op);
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		ResolveJoin(op.Cast<LogicalComparisonJoin>());
	}
}

void BitmapJoinResolver::ResolveJoin(LogicalComparisonJoin &op) {
	// Same eligibility test as plan_comparison_join.cpp's `bhj_eligible`: a single equality
	// condition on an INNER / RIGHT_SEMI join (design doc §1.3 Step C).
	bool bhj_eligible = op.conditions.size() == 1 &&
	                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
	                    (op.join_type == JoinType::INNER || op.join_type == JoinType::RIGHT_SEMI);
	if (!bhj_eligible) {
		return;
	}

	auto &cond = op.conditions[0];
	// cond.left is the probe side (LHS), cond.right is the build side (RHS) - matches the
	// existing convention used by PhysicalHashJoin / plan_comparison_join.cpp.
	auto probe_binding = GetPlainColumnBinding(*cond.left);
	auto build_binding = GetPlainColumnBinding(*cond.right);
	if (!probe_binding || !build_binding) {
		// Condition side is not a plain column reference (e.g. a cast/expression) - skip.
		return;
	}

	// Trace both sides back to their source LogicalGet, unwrapping any Filter/Projection/
	// intermediate-INNER-join chain in between (design doc §3.2 Step F, §4.3 Step A).
	auto probe_trace = TraceBindingToGet(*op.children[0], *probe_binding);
	auto build_trace = TraceBindingToGet(*op.children[1], *build_binding);
	if (!probe_trace.get || !build_trace.get) {
		// Could not trace back to a base LogicalGet (e.g. buried under an aliased subquery, a
		// computed expression, or some other operator we don't unwrap) - skip; safe fallback to
		// a regular hash join.
		return;
	}

	string build_table = ResolveLogicalTableName(*build_trace.get);
	string build_col = ResolveColumnName(*build_trace.get, build_trace.get_column_index);
	string probe_table = ResolveLogicalTableName(*probe_trace.get);
	string probe_col = ResolveColumnName(*probe_trace.get, probe_trace.get_column_index);
	if (build_table.empty() || build_col.empty() || probe_table.empty() || probe_col.empty()) {
		return;
	}

	BitmapJoinResolved resolved;
	if (!BitmapJoinMetaRegistry::Get(context).TryResolve(build_table, build_col, probe_table, probe_col, resolved)) {
		return;
	}
	if (!resolved.build_is_pk_side) {
		// The FK (fact) table landed on the build side: BHJ's core precondition (unique
		// build-side keys) is violated. Silently fall back to a regular hash join rather than
		// wiring up a hint BitmapJoinExecutor cannot support (design doc 条目2).
		return;
	}

	// 条目3 Step A: the PK column being non-dense (rowid_column != pk_column) is the single
	// switch that decides whether BOTH sides need to read a hidden materialized column instead
	// of the join key's raw value. The two sides must agree on the same mode - if the FK binding
	// doesn't carry a ref_column to match, we cannot compute probe-side rowids consistently, so
	// bail out entirely (regular hash join) rather than risk a rowid-scheme mismatch.
	if (resolved.pk->rowid_column != resolved.pk->pk_column) {
		if (!resolved.fk || resolved.fk->ref_column.empty()) {
			return;
		}
		if (!build_trace.path_complete || !probe_trace.path_complete) {
			return;
		}
		op.bhj_build_rowid_ref = PropagateHiddenColumn(*build_trace.get, build_trace.path, resolved.pk->rowid_column);
		op.bhj_probe_ref_ref = PropagateHiddenColumn(*probe_trace.get, probe_trace.path, resolved.fk->ref_column);
	}
	op.bhj_hint = make_uniq<BitmapJoinResolved>(resolved);
}

} // namespace duckdb
