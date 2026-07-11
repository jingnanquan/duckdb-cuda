// Operator-level profiling for the Bitmap-Join (BHJ) execution path (design doc
// b_idea/6.4遗漏问题/任务1-BHJ命中率归因与算子耗时画像.md, 条目7).
//
// Unlike test_bitmap_join_tpch.cpp (which only measures *query-level* wall-clock time and a
// "how many joins hit BHJ" count), this test drills down to *each individual HASH_JOIN node's*
// own operator_timing (via `PRAGMA enable_profiling='json'`) across all three modes
// (baseline / perfect / bitmap), for Q5/Q9/Q10. The goal is to answer: is the underwhelming
// end-to-end speedup a *planning* problem (hit-rate too low) or an *executor* problem (a join
// hits BHJ but doesn't actually get faster)?
//
// Results are written to b_idea/perf/sf5/tpch_operator_timing.csv (one row per HASH_JOIN node
// per mode) for the attribution table in the design doc. Hidden behind `[.]` like the other
// SF5-dataset-dependent tests; run explicitly with `./unittest "[bitmap_join_tpch_profile]"`.

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

using namespace duckdb;
using namespace duckdb_yyjson;
using namespace std;

namespace {

constexpr const char *kBitmapDataDir = "/data/workspace/database/duckdb-cuda/data/tpch_sf5_bitmap";
constexpr const char *kTables[] = {"region",  "nation",   "customer", "orders",
                                   "part",    "partsupp", "supplier", "lineitem"};

bool DatasetAvailable(const string &dir) {
	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(dir)) {
		return false;
	}
	for (auto &table : kTables) {
		if (!fs->FileExists(dir + "/" + table + ".parquet")) {
			return false;
		}
	}
	return true;
}

void CreateViews(Connection &con, const string &dir) {
	for (auto &table : kTables) {
		auto sql = StringUtil::Format("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet')",
		                              table, dir, table);
		REQUIRE_NO_FAIL(con.Query(sql));
	}
}

// Same 3 query texts as test_bitmap_join_tpch.cpp (kept independent/duplicated on purpose - this
// file's purpose, per design doc 条目7, is standalone operator-timing diagnostics and shouldn't
// need to pull in that file's helper structs).
string Q5() {
	return "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue "
	       "FROM customer, orders, lineitem, supplier, nation, region "
	       "WHERE c_custkey = o_custkey "
	       "  AND l_orderkey = o_orderkey "
	       "  AND l_suppkey = s_suppkey "
	       "  AND c_nationkey = s_nationkey "
	       "  AND s_nationkey = n_nationkey "
	       "  AND n_regionkey = r_regionkey "
	       "  AND r_name = 'ASIA' "
	       "  AND o_orderdate >= DATE '1994-01-01' "
	       "  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR "
	       "GROUP BY n_name "
	       "ORDER BY revenue DESC";
}

string Q9() {
	return "SELECT nation, o_year, sum(amount) AS sum_profit FROM ("
	       "  SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, "
	       "         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount "
	       "  FROM part, supplier, lineitem, partsupp, orders, nation "
	       "  WHERE s_suppkey = l_suppkey "
	       "    AND ps_suppkey = l_suppkey "
	       "    AND ps_partkey = l_partkey "
	       "    AND p_partkey = l_partkey "
	       "    AND o_orderkey = l_orderkey "
	       "    AND s_nationkey = n_nationkey "
	       "    AND p_name LIKE '%green%'"
	       ") AS profit "
	       "GROUP BY nation, o_year "
	       "ORDER BY nation, o_year DESC";
}

string Q10() {
	return "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, "
	       "       c_acctbal, n_name, c_address, c_phone, c_comment "
	       "FROM customer, orders, lineitem, nation "
	       "WHERE c_custkey = o_custkey "
	       "  AND l_orderkey = o_orderkey "
	       "  AND o_orderdate >= DATE '1993-10-01' "
	       "  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH "
	       "  AND l_returnflag = 'R' "
	       "  AND c_nationkey = n_nationkey "
	       "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment "
	       "ORDER BY revenue DESC "
	       "LIMIT 20";
}

string SafeStr(yyjson_val *v) {
	if (!v || !yyjson_is_str(v)) {
		return "";
	}
	auto p = yyjson_get_str(v);
	return p ? string(p) : "";
}

//! The SF5 bitmap dataset is scanned via read_parquet(...) (no catalog table, so no "Table"
//! field in extra_info) - infer which of the 8 TPCH tables a scan belongs to from a projected
//! column's unique-enough prefix. Diagnostic-only; never affects any BHJ decision.
string GuessTableFromColumn(const string &col) {
	static const std::vector<pair<string, string>> prefixes = {
	    {"l_", "lineitem"}, {"ps_", "partsupp"}, {"p_", "part"}, {"o_", "orders"},
	    {"c_", "customer"}, {"s_", "supplier"},  {"n_", "nation"}, {"r_", "region"},
	};
	for (auto &pr : prefixes) {
		if (col.size() > pr.first.size() && col.compare(0, pr.first.size(), pr.first) == 0) {
			return pr.second;
		}
	}
	return "";
}

void CollectTableNames(yyjson_val *node, std::vector<string> &out) {
	if (!node) {
		return;
	}
	string name = SafeStr(yyjson_obj_get(node, "operator_name"));
	if (name.empty()) {
		name = SafeStr(yyjson_obj_get(node, "name"));
	}
	if (name.find("SCAN") != string::npos || name.find("PARQUET") != string::npos) {
		auto extra = yyjson_obj_get(node, "extra_info");
		if (extra) {
			auto table_val = yyjson_obj_get(extra, "Table");
			if (table_val) {
				out.push_back(SafeStr(table_val));
			} else {
				auto proj = yyjson_obj_get(extra, "Projections");
				string col;
				if (proj) {
					if (yyjson_is_str(proj)) {
						col = SafeStr(proj);
					} else if (yyjson_is_arr(proj) && yyjson_arr_size(proj) > 0) {
						col = SafeStr(yyjson_arr_get(proj, 0));
					}
				}
				auto guess = GuessTableFromColumn(col);
				out.push_back(guess.empty() ? ("?" + col) : guess);
			}
		}
	}
	auto children = yyjson_obj_get(node, "children");
	if (children) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			CollectTableNames(child, out);
		}
	}
}

string JoinNames(const std::vector<string> &v) {
	string s;
	for (size_t i = 0; i < v.size(); i++) {
		if (i) {
			s += "+";
		}
		auto pos = v[i].rfind('.');
		s += pos == string::npos ? v[i] : v[i].substr(pos + 1);
	}
	return s;
}

struct JoinTiming {
	string label; //! "<condition> [<probe-side tables> <-> <build-side tables>]"
	double timing_s = 0.0;
	string bitmap_join = "(none)"; //! ParamsToString's "Bitmap Join" extra_info value, if any.
};

void WalkHashJoins(yyjson_val *node, std::vector<JoinTiming> &out) {
	if (!node) {
		return;
	}
	string name = SafeStr(yyjson_obj_get(node, "operator_name"));
	if (name.empty()) {
		name = SafeStr(yyjson_obj_get(node, "name"));
	}
	if (name == "HASH_JOIN") {
		JoinTiming jt;
		auto timing_val = yyjson_obj_get(node, "operator_timing");
		if (timing_val && yyjson_is_real(timing_val)) {
			jt.timing_s = yyjson_get_real(timing_val);
		} else if (timing_val && yyjson_is_int(timing_val)) {
			jt.timing_s = static_cast<double>(yyjson_get_int(timing_val));
		}
		auto extra = yyjson_obj_get(node, "extra_info");
		string cond;
		if (extra) {
			cond = SafeStr(yyjson_obj_get(extra, "Conditions"));
			auto bj = yyjson_obj_get(extra, "Bitmap Join");
			if (bj) {
				auto s = SafeStr(bj);
				if (!s.empty()) {
					jt.bitmap_join = s;
				}
			}
		}
		std::vector<string> lt, rt;
		auto children = yyjson_obj_get(node, "children");
		if (children && yyjson_arr_size(children) == 2) {
			CollectTableNames(yyjson_arr_get(children, 0), lt);
			CollectTableNames(yyjson_arr_get(children, 1), rt);
		}
		jt.label = cond + " [" + JoinNames(lt) + " <-> " + JoinNames(rt) + "]";
		out.push_back(jt);
	}
	auto children = yyjson_obj_get(node, "children");
	if (children) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			WalkHashJoins(child, out);
		}
	}
}

//! Runs `sql` once (untimed, to warm the OS/parquet metadata cache), then once more with JSON
//! profiling enabled, and returns each HASH_JOIN node's own (non-cumulative) operator_timing -
//! in the same top-down traversal order used elsewhere in this file, which - for a fixed SQL
//! text/statistics/join-order-optimizer input - is expected to be identical across the three
//! modes (only whether each PhysicalHashJoin is *marked* bitmap/perfect differs, not the plan
//! shape), giving a stable, positional way to line HASH_JOIN[i] up across baseline/perfect/bitmap.
std::vector<JoinTiming> RunWithProfiling(Connection &con, const string &sql, const string &tmp_path, bool open_perfect,
                                    bool open_bitmap) {
	REQUIRE_NO_FAIL(*con.Query(string("SET open_perfect_join=") + (open_perfect ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query(string("SET open_bitmap_join=") + (open_bitmap ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query("PRAGMA disable_profiling"));
	REQUIRE_NO_FAIL(*con.Query(sql)); // warm-up, untimed
	REQUIRE_NO_FAIL(*con.Query("PRAGMA enable_profiling='json'"));
	REQUIRE_NO_FAIL(*con.Query("PRAGMA profiling_output='" + tmp_path + "'"));
	auto result = con.Query(sql);
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(*con.Query("PRAGMA disable_profiling"));

	std::ifstream f(tmp_path);
	std::stringstream ss;
	ss << f.rdbuf();
	string json_text = ss.str();
	yyjson_doc *doc = yyjson_read(json_text.c_str(), json_text.size(), 0);
	REQUIRE(doc != nullptr);
	std::vector<JoinTiming> out;
	WalkHashJoins(yyjson_doc_get_root(doc), out);
	yyjson_doc_free(doc);
	return out;
}

void RunAndReport(Connection &con, const string &name, const string &sql, std::ofstream &csv, const string &tmp_dir) {
	auto baseline = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_baseline.json", false, false);
	auto perfect = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_perfect.json", true, false);
	auto bitmap = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_bitmap.json", false, true);

	printf("\n=== %s ===\n", name.c_str());
	printf("%-70s %12s %12s %12s %10s\n", "Join (condition [probe <-> build])", "baseline(ms)", "perfect(ms)",
	       "bitmap(ms)", "BHJ?");
	size_t n = std::max({baseline.size(), perfect.size(), bitmap.size()});
	for (size_t i = 0; i < n; i++) {
		string label = i < bitmap.size() ? bitmap[i].label : (i < baseline.size() ? baseline[i].label : "?");
		double b = i < baseline.size() ? baseline[i].timing_s * 1000.0 : -1;
		double p = i < perfect.size() ? perfect[i].timing_s * 1000.0 : -1;
		double m = i < bitmap.size() ? bitmap[i].timing_s * 1000.0 : -1;
		string bj = i < bitmap.size() ? bitmap[i].bitmap_join : "?";
		printf("%-70s %12.3f %12.3f %12.3f %10s\n", label.c_str(), b, p, m, bj.c_str());
		csv << name << ",\"" << label << "\"," << b << "," << p << "," << m << ",\"" << bj << "\"\n";
	}
}

} // namespace

TEST_CASE("BHJ operator-level timing across baseline/perfect/bitmap for TPC-H Q5/Q9/Q10 (SF=5)",
          "[bitmap_join_tpch_profile][.]") {
	if (!DatasetAvailable(kBitmapDataDir)) {
		std::cerr << "[bitmap-join-tpch-profile] dataset directory '" << kBitmapDataDir
		          << "' not found/incomplete - skipping test." << std::endl;
		return;
	}
	const string meta_path = string(kBitmapDataDir) + "/bitmap_join_meta.json";
	{
		auto fs = FileSystem::CreateLocal();
		if (!fs->FileExists(meta_path)) {
			std::cerr << "[bitmap-join-tpch-profile] meta file '" << meta_path << "' not found - skipping test."
			          << std::endl;
			return;
		}
	}

	DuckDB db(nullptr);
	Connection con(db);
	CreateViews(con, kBitmapDataDir);

	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");
	registry.LoadFromJson(meta_path);

	auto fs = FileSystem::CreateLocal();
	const string csv_dir = "/data/workspace/database/duckdb-cuda/b_idea/perf/sf5";
	fs->CreateDirectoriesRecursive(csv_dir);
	std::ofstream csv(csv_dir + "/tpch_operator_timing.csv", std::ios::out | std::ios::trunc);
	csv << "query,join_label,baseline_ms,perfect_ms,bitmap_ms,bitmap_join_status\n";

	RunAndReport(con, "Q5", Q5(), csv, "/tmp");
	RunAndReport(con, "Q9", Q9(), csv, "/tmp");
	RunAndReport(con, "Q10", Q10(), csv, "/tmp");

	printf("\nCSV written to b_idea/perf/sf5/tpch_operator_timing.csv\n");
	registry.Clear();
}
