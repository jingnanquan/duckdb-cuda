# task_list

- [x] 1. 建立调研文档骨架与源码引用规范
- [x] 2. 梳理 Scan Operator 到数据输出的执行入口
- [x] 3. 梳理 Parquet 文件扫描的数据读取路径
- [x] 4. 梳理内存表扫描路径并与 Parquet scan 对比
- [x] 5. 梳理 Parquet predicate pushdown 与提前裁剪逻辑
- [x] 6. 梳理三种运行/存储模式下的数据完整性与溢出策略
- [x] 7. 比较大于内存容量场景下持久化模式与内存模式被动溢出
- [x] 8. 梳理 Parquet 默认配置与兼容性
- [x] 9. 编写并校对最终 DuckDB scan 流程文档

## 当前进度记录

- 已完成全部 9 项任务。
- 最终文档已创建：`.codebuddy/plan/duckdb_scan_flow/duckdb_scan_flow.md`。
