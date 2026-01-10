# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-10

### Added
- 基础 KV 操作 (Get/Put/Delete/BatchGet/BatchPut)
- 存算分离三层架构 (Compute/Storage/Metadata)
- 固定 4096 分区路由 (基于 xxhash)
- 双副本同步写入 (Primary + Replica)
- 自动故障检测与迁移
- 支持 4 个 Storage 节点 (storage-1/2/3/4)
- 支持 2 个 Compute 节点 (compute-1/2)
- 基于 etcd 的元数据存储
- 命令行客户端 (rockskv-cli)
- Prometheus metrics 支持
- 结构化日志 (zap)
- Docker Compose 部署支持

### Performance
- etcd 写入优化：心跳使用 Lease (减少 87% 写入)
- etcd 写入优化：迁移状态分离 (减少 99.95% 写入)
- etcd 自动压缩：每 5 分钟压缩历史版本
- RocksDB LZ4 压缩
- gRPC 连接池复用
- 总体 etcd 写入放大减少 99%+

### Infrastructure
- 一键启动脚本 (start-all.sh/stop-all.sh/status.sh)
- 冒烟测试脚本 (smoke-test.sh)
- 压力测试脚本 (stress-test.sh)
- 分区重平衡脚本 (trigger-rebalance.sh)
- 支持 macOS (Intel/Apple Silicon) 和 Linux (x86_64/ARM64)

### Documentation
- README.md - 项目概述和快速开始
- CLAUDE.md - Claude Code 开发指南
- DEPENDENCIES.md - 依赖版本和兼容性
- docs/ETCD_OPTIMIZATION.md - etcd 优化设计文档
- VERSION - 版本号文件
- CHANGELOG.md - 变更日志

### Dependencies
- Go 1.24.0
- etcd client v3.5.11
- gRPC v1.78.0
- Protobuf v1.36.10
- grocksdb (RocksDB) v1.10.4
- zap v1.26.0
- prometheus/client_golang v1.18.0

## [0.0.1] - 2026-01-09 (Internal)

### Added
- 初始项目结构
- 基础 Protobuf 定义
- Storage 层 stub 实现
- Compute 层基础路由
- Metadata 层基础实现

---

## 版本说明

### 语义化版本

- **MAJOR** (主版本号): 不兼容的 API 变更
- **MINOR** (次版本号): 向后兼容的功能新增
- **PATCH** (修订号): 向后兼容的问题修正

### 版本计划

- **v0.1.x**: 基础功能和性能优化
- **v0.2.x**: 范围查询 (Scan) 支持
- **v0.3.x**: 事务支持
- **v1.0.0**: 生产就绪版本

## 链接

- [当前版本](VERSION)
- [依赖信息](DEPENDENCIES.md)
- [GitHub Releases](https://github.com/winewei/rockskv/releases)
