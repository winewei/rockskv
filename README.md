# RocksKV

基于 RocksDB 的存算分离分布式 KV 数据库，作为 DynamoDB 的低成本替代方案。

## 核心特性

### 存算分离架构
- **Compute Layer**：无状态路由层，可独立水平扩展
- **Storage Layer**：有状态存储层，基于 RocksDB，WAL 存 EBS，SST 存 NVMe
- **Metadata Service**：集中式路由管理，基于 etcd

### 高可用性
- **双副本同步写入**：Primary + Replica，写入必须两个副本都成功
- **自动故障切换**：通过心跳检测节点健康状态，自动标记故障节点
- **在线扩缩容**：支持动态添加/移除存储节点，自动触发分区迁移

### 高性能
- **RocksDB 存储引擎**：NVMe SSD 优化，LZ4 压缩
- **固定分区方案**：4096 个固定分区，基于 xxhash 的一致性路由
- **连接池管理**：Compute 层维护到 Storage 的连接池，复用 gRPC 连接
- **性能目标**：
  - 读 P99 延迟 < 10ms
  - 写 P99 延迟 < 15ms
  - 单节点读 QPS > 10,000
  - 单节点写 QPS > 15,000

### 运维友好
- **自动分区平衡**：新增节点时自动迁移分区，均衡负载
- **Prometheus Metrics**：所有服务暴露 Prometheus 指标
- **结构化日志**：基于 zap 的高性能日志，支持日志级别控制
- **脚本化部署**：提供一键启动/停止脚本，支持多节点本地开发

## 架构图

```
                    ┌─────────────┐
                    │   Client    │
                    │   (SDK)     │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       ┌────────────┐            ┌────────────┐
       │ Compute-1  │            │ Compute-2  │
       │  (:8000)   │            │  (:8001)   │
       └─────┬──────┘            └──────┬─────┘
             │                          │
             └────────────┬─────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
  ┌────────────┐   ┌────────────┐   ┌────────────┐
  │ Storage-1  │   │ Storage-2  │   │  Metadata  │
  │  (:9001)   │   │  (:9002)   │   │  (:9000)   │
  └─────┬──────┘   └─────┬──────┘   └──────┬─────┘
        │                │                 │
        ▼                ▼                 ▼
     RocksDB          RocksDB            etcd
```

### 三层架构

**Compute Layer (无状态)**
- 接收客户端请求，通过 xxhash(key) % 4096 计算分区 ID
- 查询路由表获取 Primary 和 Replica 节点
- 协调双副本写入（必须两个副本都成功）
- 可水平扩展，无状态，随时增减

**Storage Layer (有状态)**
- 基于 RocksDB 的 KV 存储引擎
- 每个节点负责多个分区（Primary 或 Replica）
- 订阅路由表更新，动态加载/卸载分区
- 支持 SST 导入/导出，用于分区迁移

**Metadata Service (协调层)**
- 路由表管理：维护 4096 个分区的 Primary/Replica 映射
- 节点注册与心跳：15 秒心跳超时检测
- 故障检测与迁移：自动触发故障节点的分区迁移
- 基于 etcd 存储，5 分钟自动压缩历史版本

## 快速开始

### 环境要求

- Go 1.21+
- etcd 3.5+（本地开发）
- macOS (Intel/Apple Silicon) 或 Linux (x86_64/ARM64)

### 安装

```bash
# 克隆仓库
git clone https://github.com/winewei/rockskv.git
cd rockskv

# 下载依赖
go mod download
```

### 构建

```bash
# 构建当前平台
make build

# 构建 macOS Apple Silicon (M1/M2)
make build-darwin-arm64

# 构建 Linux x86_64
make build-linux-amd64

# 构建所有平台
make build-all
```

### 本地运行

**方式一：一键启动**

```bash
# 安装 etcd (macOS)
brew install etcd

# 构建
make build-darwin-arm64  # M1/M2 Mac
# 或
make build-darwin-amd64  # Intel Mac

# 启动所有服务（etcd + metadata + 4 storage + 2 compute）
./scripts/start-all.sh

# 查看状态
./scripts/status.sh

# 停止服务
./scripts/stop-all.sh
```

**方式二：Docker Compose**

```bash
docker-compose up -d
```

### 使用 CLI

```bash
# 写入数据 (默认连接 localhost:8000)
./bin/darwin-arm64/rockskv-cli put mykey "Hello, RocksKV!"

# 读取数据
./bin/darwin-arm64/rockskv-cli get mykey

# 删除数据
./bin/darwin-arm64/rockskv-cli delete mykey

# 连接到其他 Compute 节点
./bin/darwin-arm64/rockskv-cli -addr localhost:8001 get mykey

# 批量操作
./bin/darwin-arm64/rockskv-cli mset k1 v1 k2 v2
./bin/darwin-arm64/rockskv-cli mget k1 k2
```

## 配置

配置文件位于 `config/` 目录：

| 文件 | 说明 |
|------|------|
| `config/metadata.yaml` | Metadata 服务配置 |
| `config/storage.yaml` | Storage 服务配置 |
| `config/compute.yaml` | Compute 服务配置 |
| `config/local/` | 本地开发配置（4 storage + 2 compute） |

### 主要配置项

**Storage 配置 (`storage.yaml`)**

```yaml
node_id: "storage-1"
listen_addr: ":9001"
metadata_addr: "localhost:9000"
metrics_addr: ":9091"

rocksdb:
  sst_dir: "./data/sst1"        # SST 文件目录（NVMe）
  wal_dir: "./data/wal1"        # WAL 文件目录（EBS）
  block_cache_size: 536870912   # 512MB
  write_buffer_size: 67108864   # 64MB
  compression_type: "lz4"       # LZ4 压缩
```

**Compute 配置 (`compute.yaml`)**

```yaml
node_id: "compute-1"
listen_addr: ":8000"
metadata_addr: "localhost:9000"
metrics_addr: ":8090"

pool:
  max_conns_per_host: 10        # 每个 Storage 的最大连接数
  dial_timeout: "5s"            # 连接超时
```

**Metadata 配置 (`metadata.yaml`)**

```yaml
node_id: "metadata-1"
listen_addr: ":9000"
metrics_addr: ":9090"

etcd:
  endpoints: ["localhost:2379"]
  dial_timeout: "5s"
```

## 服务端口

| 服务 | 端口 | Metrics 端口 | 说明 |
|------|------|-------------|------|
| etcd | 2379 | - | Metadata 存储 |
| Metadata | 9000 | 9090 | 路由表管理 |
| Storage-1 | 9001 | 9091 | 数据存储节点 1 |
| Storage-2 | 9002 | 9092 | 数据存储节点 2 |
| Storage-3 | 9003 | 9093 | 数据存储节点 3 |
| Storage-4 | 9004 | 9094 | 数据存储节点 4 |
| Compute-1 | 8000 | 8090 | 客户端入口 1 |
| Compute-2 | 8001 | 8091 | 客户端入口 2 |

## 目录结构

```
rockskv/
├── cmd/                    # 服务入口
│   ├── compute/main.go     # Compute 服务
│   ├── storage/main.go     # Storage 服务
│   ├── metadata/main.go    # Metadata 服务
│   └── cli/main.go         # 命令行客户端
├── pkg/                    # 核心代码
│   ├── compute/            # Compute 层实现
│   │   └── router.go       # 路由和双副本写入
│   ├── storage/            # Storage 层实现
│   │   ├── server_stub.go  # gRPC 服务（stub 模式）
│   │   ├── partition.go    # 分区管理
│   │   └── rocksdb_stub.go # RocksDB 封装（stub 模式）
│   ├── metadata/           # Metadata 服务
│   │   ├── server.go       # 元数据服务
│   │   ├── router.go       # 路由表管理
│   │   ├── migration.go    # 分区迁移控制器
│   │   ├── etcd.go         # etcd 存储实现
│   │   └── etcd_compact.go # 自动压缩
│   ├── client/             # 客户端 SDK
│   ├── proto/              # Protobuf 生成代码
│   └── common/             # 公共组件（日志、metrics）
├── config/                 # 配置文件
│   ├── local/              # 本地开发配置
│   ├── storage.yaml        # Storage 模板配置
│   ├── compute.yaml        # Compute 模板配置
│   └── metadata.yaml       # Metadata 配置
├── scripts/                # 运维脚本
│   ├── start-all.sh        # 一键启动所有服务
│   ├── stop-all.sh         # 一键停止所有服务
│   ├── status.sh           # 查看服务状态
│   └── smoke-test.sh       # 冒烟测试
├── proto/                  # Proto 源文件
│   └── rockskv.proto       # 服务定义
└── docs/                   # 设计文档
```

## 核心设计

### 分区路由

- **固定分区数**：4096 个分区（可配置）
- **分区计算**：`partition_id = xxhash(key) % 4096`
- **双副本写入**：每个分区有 Primary 和 Replica，写入时必须两者都成功
- **路由表**：Metadata 维护全局路由表，Storage/Compute 订阅更新

### 数据分布

每个分区的 Key 编码为：`p:<partition_id_4bytes>:<user_key>`

示例：
- Key `"user:123"` → Partition 2048 → RocksDB Key: `p:0x0800:user:123`

### 故障处理

1. **心跳检测**：Storage 每 10 秒向 Metadata 发送心跳
2. **故障标记**：心跳超时 15 秒后标记节点为故障
3. **自动迁移**：故障节点的分区自动迁移到健康节点
4. **分区平衡**：新增节点时自动平衡分区分布

### 分区迁移

1. **触发条件**：
   - 新增 Storage 节点
   - Storage 节点故障
   - 手动负载均衡

2. **迁移流程**：
   - 设置迁移状态（COPYING）
   - 源节点导出 SST 文件
   - 目标节点导入 SST 文件
   - 切换路由表
   - 清理迁移状态

3. **并发控制**：
   - 使用 etcd CAS 事务防止并发冲突
   - 迁移状态独立存储，减少路由表写入

## 开发

### 运行测试

```bash
# 运行所有测试
make test

# 运行特定包测试
make test-storage
make test-compute
make test-metadata

# 运行 e2e 测试
make test-e2e

# 冒烟测试（需要服务运行）
./scripts/smoke-test.sh
```

### 代码检查

```bash
# 格式化
make fmt

# Lint 检查
make lint
```

### 生成 Protobuf

```bash
# 需要安装 protoc 和 Go 插件
make proto
```

### 本地开发

```bash
# 启动所有服务
./scripts/start-all.sh

# 查看日志
tail -f logs/metadata.log
tail -f logs/storage.log
tail -f logs/compute.log

# 重启某个服务
./scripts/stop-all.sh
./scripts/start-storage-3.sh

# 查看 metrics
curl localhost:9090/metrics  # metadata
curl localhost:9091/metrics  # storage-1
curl localhost:8090/metrics  # compute-1
```

## 监控指标

所有服务暴露 Prometheus 指标（`/metrics` 端点）：

**Compute Metrics**
- `rockskv_requests_total{method, status}` - 请求总数
- `rockskv_request_duration_seconds{method}` - 请求延迟分布

**Storage Metrics**
- `rockskv_storage_partitions{role}` - 分区数量（按 primary/replica）
- `rockskv_storage_operations_total{operation, status}` - 操作计数

**Metadata Metrics**
- `rockskv_route_table_version` - 路由表版本号
- `rockskv_active_nodes{role}` - 活跃节点数

## 性能优化

### etcd 优化

- **心跳独立存储**：心跳使用 Lease 自动过期，减少 87% 写入
- **迁移状态分离**：迁移状态独立存储，减少 99.95% 写入
- **自动压缩**：每 5 分钟压缩历史版本，保留最近 100 个修订

### RocksDB 优化

- **NVMe 优化**：SST 文件存储在 NVMe SSD
- **WAL 分离**：WAL 存储在 EBS，保证持久性
- **LZ4 压缩**：平衡压缩率和性能
- **Block Cache**：512MB 默认缓存

### 网络优化

- **连接池复用**：Compute 维护到 Storage 的长连接池
- **gRPC 流式传输**：分区迁移使用流式 API
- **超时控制**：所有 RPC 调用 5 秒超时

## 路线图

- [x] 基础 KV 操作 (Get/Put/Delete)
- [x] 固定分区路由（4096 分区）
- [x] 双副本同步写入
- [x] 批量操作 (BatchGet/BatchPut)
- [x] 多 Compute 节点支持
- [x] 多 Storage 节点支持（4+ 节点）
- [x] 自动故障检测与迁移
- [x] etcd 写入优化（心跳 Lease + 迁移状态分离）
- [ ] 范围查询 (Scan)
- [ ] 事务支持 (Transaction)
- [ ] 在线扩缩容优化（批量迁移）
- [ ] Grafana 监控面板
- [ ] 告警规则

## 文档

- [CLAUDE.md](CLAUDE.md) - Claude Code 开发指南
- [docs/ETCD_OPTIMIZATION.md](docs/ETCD_OPTIMIZATION.md) - etcd 优化设计文档

## 贡献

欢迎提交 Issue 和 Pull Request！

开发前请先阅读 [CLAUDE.md](CLAUDE.md) 了解项目结构和开发规范。

## 许可证

MIT License
