# CLAUDE.md - RocksKV 开发指南

## 项目简介

RocksKV 是一个基于 RocksDB 的存算分离分布式 KV 数据库，作为 DynamoDB 的低成本替代方案。

## 架构概览

```
Client → Compute Layer → Storage Layer → RocksDB + NVMe
              ↓
         Metadata Service (etcd)
```

- **Compute Layer**: 无状态路由层，负责请求路由、双副本写入协调
- **Storage Layer**: 有状态存储层，基于 RocksDB，WAL 存 EBS，SST 存 NVMe
- **Metadata Service**: 路由表管理、节点注册、故障检测

## 技术栈

- 语言: Go 1.21+
- 存储引擎: RocksDB (grocksdb)
- RPC: gRPC + Protobuf
- 元数据: etcd
- 日志: zap
- 配置: viper
- 监控: prometheus

## 目录结构

```
rockskv/
├── cmd/
│   ├── compute/main.go      # Compute 服务入口
│   ├── storage/main.go      # Storage 服务入口
│   ├── metadata/main.go     # Metadata 服务入口
│   └── cli/main.go          # CLI 客户端
├── proto/
│   └── rockskv.proto        # Protobuf 定义
├── pkg/
│   ├── compute/             # Compute 层实现
│   │   ├── router.go        # 路由模块
│   │   ├── pool.go          # 连接池
│   │   └── server.go        # gRPC 服务
│   ├── storage/             # Storage 层实现
│   │   ├── rocksdb.go       # RocksDB 封装
│   │   ├── partition.go     # 分区管理
│   │   ├── server.go        # gRPC 服务
│   │   └── migration.go     # SST 迁移
│   ├── metadata/            # Metadata 服务实现
│   │   ├── store.go         # 存储接口
│   │   ├── etcd.go          # etcd 实现
│   │   ├── router.go        # 路由管理
│   │   ├── server.go        # gRPC 服务
│   │   └── failover.go      # 故障切换
│   ├── client/              # 客户端 SDK
│   │   └── client.go
│   └── common/              # 公共组件
│       ├── logger.go
│       └── metrics.go
├── config/                  # 配置文件
│   ├── storage.yaml
│   ├── compute.yaml
│   └── metadata.yaml
└── Makefile
```

## 核心设计

### 分区策略
- 固定 4096 个分区
- 路由: `partition_id = xxhash(key) % 4096`
- 每个分区有 Primary 和 Replica 两个副本

### 双副本同步写入
Compute 层并行写入 Primary 和 Replica，两边都成功才返回成功。

### RocksDB WAL 分离
WAL 写入 EBS（持久），数据存 NVMe（高性能）。

### SST Ingest 快速迁移
通过导出和加载 SST 文件实现分区快速迁移。

## 常用命令

```bash
# 生成 protobuf (需要 protoc)
make proto

# 构建所有服务
make build

# 构建特定服务
make build-storage
make build-compute
make build-metadata
make build-cli

# 运行测试
make test

# 运行特定测试
make test-storage
make test-compute

# 清理
make clean
```

## 运行服务

```bash
# 启动 metadata 服务 (需要先启动 etcd)
./bin/rockskv-metadata -c config/metadata.yaml

# 启动 storage 服务
./bin/rockskv-storage -c config/storage.yaml

# 启动 compute 服务
./bin/rockskv-compute -c config/compute.yaml

# 使用 CLI
./bin/rockskv-cli put foo bar
./bin/rockskv-cli get foo
```

## 性能目标

| 指标 | 目标 |
|------|------|
| 读 P99 | < 10ms |
| 写 P99 | < 15ms |
| 单节点读 QPS | > 10,000 |
| 单节点写 QPS | > 15,000 |

## 注意事项

1. 所有 gRPC 调用设置 5s 超时
2. 连接池复用连接，避免频繁创建
3. RocksDB 配置 Block Cache 为内存的 60%
4. Compaction 线程数限制为 CPU 核数的一半
5. 优雅关闭：先停止接收新请求，等待进行中请求完成，再关闭
