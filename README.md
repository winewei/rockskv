# RocksKV

基于 RocksDB 的存算分离分布式 KV 数据库，作为 DynamoDB 的低成本替代方案。

## 特性

- **存算分离架构**：Compute 层无状态，Storage 层有状态，独立扩展
- **高性能**：基于 RocksDB，NVMe 存储，读 P99 < 10ms，写 P99 < 15ms
- **高可用**：双副本同步写入，自动故障切换
- **易扩展**：固定 4096 分区，支持在线扩缩容
- **兼容性**：提供类 DynamoDB API

## 架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│   Compute   │────▶│   Storage   │
│   (SDK)     │     │   (Router)  │     │  (RocksDB)  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │                    │
                           ▼                    ▼
                    ┌─────────────┐      ┌─────────────┐
                    │  Metadata   │      │    NVMe     │
                    │   (etcd)    │      │    + EBS    │
                    └─────────────┘      └─────────────┘
```

- **Compute Layer**：无状态路由层，负责请求路由、双副本写入协调
- **Storage Layer**：有状态存储层，基于 RocksDB，WAL 存 EBS，SST 存 NVMe
- **Metadata Service**：路由表管理、节点注册、故障检测（基于 etcd）

## 快速开始

### 环境要求

- Go 1.21+
- etcd 3.5+（本地开发）
- macOS (Intel/Apple Silicon) 或 Linux (x86_64/ARM64)

### 安装

```bash
# 克隆仓库
git clone https://github.com/example/rockskv.git
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

# 启动所有服务
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
# 写入数据
./bin/darwin-arm64/rockskv-cli put mykey "Hello, RocksKV!"

# 读取数据
./bin/darwin-arm64/rockskv-cli get mykey

# 删除数据
./bin/darwin-arm64/rockskv-cli delete mykey
```

## 配置

配置文件位于 `config/` 目录：

| 文件 | 说明 |
|------|------|
| `config/metadata.yaml` | Metadata 服务配置 |
| `config/storage.yaml` | Storage 服务配置 |
| `config/compute.yaml` | Compute 服务配置 |
| `config/local/` | 本地开发配置 |

### 主要配置项

**Storage 配置 (`storage.yaml`)**

```yaml
node_id: "storage-1"
listen_addr: ":9001"
metadata_addr: "localhost:9000"

rocksdb:
  data_dir: "/data/rockskv"
  wal_dir: "/data/rockskv/wal"
  block_cache_size: 536870912  # 512MB
  write_buffer_size: 67108864  # 64MB
  compression_type: "lz4"
```

**Compute 配置 (`compute.yaml`)**

```yaml
node_id: "compute-1"
listen_addr: ":8000"
metadata_addr: "localhost:9000"

pool:
  max_conns_per_host: 10
  dial_timeout: "5s"
```

## 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| etcd | 2379 | Metadata 存储 |
| Metadata | 9000 | 路由表管理 |
| Storage | 9001 | 数据存储 |
| Compute | 8000 | 客户端入口 |

## 目录结构

```
rockskv/
├── cmd/                    # 服务入口
│   ├── compute/main.go
│   ├── storage/main.go
│   ├── metadata/main.go
│   └── cli/main.go
├── pkg/                    # 核心代码
│   ├── compute/            # Compute 层实现
│   ├── storage/            # Storage 层实现
│   ├── metadata/           # Metadata 服务
│   ├── client/             # 客户端 SDK
│   ├── proto/              # Protobuf 定义
│   └── common/             # 公共组件
├── config/                 # 配置文件
│   ├── local/              # 本地开发配置
│   ├── storage.yaml
│   ├── compute.yaml
│   └── metadata.yaml
├── scripts/                # 启动脚本
├── deploy/                 # Docker 部署
└── proto/                  # Proto 源文件
```

## 开发

### 运行测试

```bash
# 运行所有测试
make test

# 运行特定包测试
make test-storage
make test-compute
make test-metadata

# 运行测试（不需要 CGO）
CGO_ENABLED=0 go test -v ./...
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

## 性能目标

| 指标 | 目标 |
|------|------|
| 读 P99 延迟 | < 10ms |
| 写 P99 延迟 | < 15ms |
| 单节点读 QPS | > 10,000 |
| 单节点写 QPS | > 15,000 |

## 路线图

- [x] 基础 KV 操作 (Get/Put/Delete)
- [x] 分区路由
- [x] 双副本写入
- [ ] 批量操作 (BatchGet/BatchPut)
- [ ] 范围查询 (Scan)
- [ ] 事务支持
- [ ] 在线扩缩容
- [ ] 监控告警

## 许可证

MIT License
