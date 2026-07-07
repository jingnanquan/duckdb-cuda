"tpch_Q05": {
    "wall_time_s": 0.4173870086669922,
    "profile_total_time_s": 1.350087823000002,
    "scan_ratio_pct": 85.60819624546735,
    "type_summary": {
      "TABLE_SCAN": {
        "time_s": 1.1557858329999995,
        "pct": 85.60819624546735,
        "count": 6
      },
      "HASH_JOIN": {
        "time_s": 0.187715503000002,
        "pct": 13.903947565639346,
        "count": 5
      },
      "HASH_GROUP_BY": {
        "time_s": 0.00414584,
        "pct": 0.30707928250086836,
        "count": 1
      },
      "PROJECTION": {
        "time_s": 0.0022922999999999997,
        "pct": 0.16978895453677434,
        "count": 2
      },
      "ORDER_BY": {
        "time_s": 0.00014834699999999998,
        "pct": 0.01098795185563271,
        "count": 1
      }
    },}

"tpch_Q09": {
    "wall_time_s": 0.8770058155059814,
    "profile_total_time_s": 3.0064480950000014,
    "scan_ratio_pct": 48.07231435006696,
    "type_summary": {
      "TABLE_SCAN": {
        "time_s": 1.4452691790000005,
        "pct": 48.07231435006696,
        "count": 6
      },
      "HASH_JOIN": {
        "time_s": 1.4050183600000001,
        "pct": 46.73349798842941,
        "count": 5
      },
      "HASH_GROUP_BY": {
        "time_s": 0.0948813790000002,
        "pct": 3.1559293891618023,
        "count": 1
      },
      "PROJECTION": {
        "time_s": 0.060995375000000046,
        "pct": 2.028818495201728,
        "count": 9
      },
      "ORDER_BY": {
        "time_s": 0.000283802,
        "pct": 0.009439777140073987,
        "count": 1
      }
    },}

后续问题：
1. 10暂不知晓，需要重新分析一下典型的查询，看看哪些hashjoin的占比高，进一步分析hashjoin的瓶颈
2. 需要拿到10的执行计划，并分析一下为什么3个hashjoin只有一个命中bitmapjoin。并且为什么5和9命中率也只有3/5
3. 需要对5，9，10这几个查询，profling出每个hashjoin实际耗费的时间，这个很重要，看看差距不大的原因是执行计划上还是执行过程中。
4. 左侧传播导致绝对位置漂移的严重 bug（用真实 TPCH Q5 才复现出来）：DuckDB 的 LogicalJoin::GetColumnBindings() 固定按 [left部分][right部分] 拼接。如果隐藏列需要穿过中间 join 的左侧，往 left_projection_map 追加新条目会把 right 部分的所有绝对位置整体后移，导致早于我们运行的、且引用了该 join 右侧输出的祖先节点（由 RemoveUnusedColumns 生成）全部错位——这正是最初 TPCH Q5 崩溃（Failed to bind column reference [29.1]）的根因。修复方案：只允许通过中间 join 的右侧传播（因为 right 是布局的最后一段，追加永远安全），左侧传播则安全放弃（回退普通 hash join）。
我认为静默放弃左侧传播并不好，这意味着上层的join，本来可以走bitmapjoin优化，但是因为下层join的原因不得不放弃。应该有较好的方法，当下层join左侧传播破坏位置时能够通知到上层join去更新位置。或者，整个绑定的过程直接自下而上这样上层join在binding隐藏列时，下层join的隐藏列已经固化了。并且这一类问题，应该需要在test_bitmap_join_chain.cpp中测试出来。