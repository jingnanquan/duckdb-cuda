#include "duckdb/optimizer/bitmap_join_resolver.hpp"

#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

//! DFS the (still un-flattened) logical plan for the LogicalGet whose table_index matches.
//! Runs before ColumnBindingResolver, so table_index values on LogicalGet are still the
//! original binder-assigned indices.
optional_ptr<LogicalGet> FindSourceGet(LogicalOperator &op, idx_t table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_index == table_index) {
			return &get;
		}
		return nullptr;
	}
	for (auto &child : op.children) {
		auto result = FindSourceGet(*child, table_index);
		if (result) {
			return result;
		}
	}
	return nullptr;
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

//! Returns the ColumnBinding of `expr` if it is (still) a plain BoundColumnRefExpression, or
//! nullptr otherwise (e.g. the condition side is a computed expression such as a cast).
optional_ptr<const ColumnBinding> GetPlainColumnBinding(Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &expr.Cast<BoundColumnRefExpression>().binding;
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

	auto build_get = FindSourceGet(*op.children[1], build_binding->table_index);
	auto probe_get = FindSourceGet(*op.children[0], probe_binding->table_index);
	if (!build_get || !probe_get) {
		// Could not trace back to a base LogicalGet (e.g. buried under an aliased subquery) -
		// skip; safe fallback to a regular hash join.
		return;
	}

	string build_table = ResolveLogicalTableName(*build_get);
	string build_col = ResolveColumnName(*build_get, build_binding->column_index);
	string probe_table = ResolveLogicalTableName(*probe_get);
	string probe_col = ResolveColumnName(*probe_get, probe_binding->column_index);
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
	op.bhj_hint = make_uniq<BitmapJoinResolved>(resolved);
}

} // namespace duckdb
