# 依赖版本

本文档记录了 RocksKV 项目的所有关键依赖及其版本要求。

## 核心依赖

### 运行时依赖

| 组件 | 版本 | 说明 |
|------|------|------|
| **Go** | 1.24.0+ | Go 编译器和运行时 |
| **etcd** | 3.5.11+ | 元数据存储（推荐 v3.5.12） |
| **RocksDB** | 7.x+ | 底层 KV 存储引擎（通过 grocksdb） |

### Go 依赖库

| 包 | 版本 | 用途 |
|------|------|------|
| `go.etcd.io/etcd/client/v3` | v3.5.11 | etcd 客户端 |
| `google.golang.org/grpc` | v1.78.0 | gRPC 框架 |
| `google.golang.org/protobuf` | v1.36.10 | Protobuf 运行时 |
| `github.com/linxGnu/grocksdb` | v1.10.4 | RocksDB Go 绑定 |
| `go.uber.org/zap` | v1.26.0 | 结构化日志 |
| `github.com/prometheus/client_golang` | v1.18.0 | Prometheus metrics |
| `github.com/spf13/viper` | v1.18.2 | 配置管理 |
| `github.com/cespare/xxhash/v2` | v2.3.0 | 哈希函数（分区路由） |

## 开发工具

### 必需工具

| 工具 | 版本 | 用途 |
|------|------|------|
| **protoc** | 3.19.0+ | Protobuf 编译器 |
| **protoc-gen-go** | v1.31.0+ | Protobuf Go 插件 |
| **protoc-gen-go-grpc** | v1.3.0+ | gRPC Go 插件 |

安装方法：

```bash
# macOS
brew install protobuf

# 安装 Go 插件
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
```

### 可选工具

| 工具 | 版本 | 用途 |
|------|------|------|
| **golangci-lint** | v1.55.0+ | 代码检查 |
| **docker** | 20.10+ | 容器运行时 |
| **docker-compose** | v2.0+ | 容器编排 |

## 平台支持

### 操作系统

| 平台 | 架构 | 状态 |
|------|------|------|
| **macOS** | ARM64 (M1/M2) | ✅ 完全支持 |
| **macOS** | AMD64 (Intel) | ✅ 完全支持 |
| **Linux** | AMD64 | ✅ 完全支持 |
| **Linux** | ARM64 | ✅ 完全支持 |

### RocksDB 支持

RocksDB 通过 `grocksdb` 集成，项目使用 stub 模式（`CGO_ENABLED=0`）编译，不需要本地安装 RocksDB。

- **开发模式**: 使用 stub 实现，无需 CGO
- **生产模式**: 需要编译支持 CGO 的版本并链接 RocksDB 库

## Docker 镜像版本

### docker-compose.yml

```yaml
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.12
```

### 推荐的基础镜像（生产环境）

```dockerfile
# Metadata/Compute 服务
FROM golang:1.24-alpine AS builder
FROM alpine:3.19

# Storage 服务（需要 RocksDB）
FROM golang:1.24 AS builder
FROM ubuntu:22.04
```

## 版本兼容性

### etcd 客户端兼容性

| etcd Server | 客户端版本 | 兼容性 |
|-------------|-----------|--------|
| v3.5.x | v3.5.11 | ✅ 完全兼容 |
| v3.4.x | v3.5.11 | ⚠️ 降级兼容 |
| v3.6.x | v3.5.11 | ⚠️ 升级后测试 |

### gRPC 兼容性

- **最低要求**: v1.50.0
- **推荐版本**: v1.78.0
- **协议版本**: HTTP/2

### Go 版本兼容性

- **最低要求**: Go 1.21
- **推荐版本**: Go 1.24
- **工具链**: Go 1.24.7

## 更新历史

### v0.1.0 (2026-01-10)
- 初始版本
- etcd client: v3.5.11
- gRPC: v1.78.0
- grocksdb: v1.10.4
- 支持 4 storage + 2 compute 节点

## 依赖升级指南

### 升级 etcd

```bash
# 1. 更新 go.mod
go get go.etcd.io/etcd/client/v3@v3.5.12

# 2. 更新 docker-compose.yml
# 修改 image: quay.io/coreos/etcd:v3.5.12

# 3. 运行测试
make test
```

### 升级 gRPC

```bash
# 1. 更新 go.mod
go get google.golang.org/grpc@v1.79.0

# 2. 重新生成 proto 代码
make proto

# 3. 运行测试
make test
```

### 升级 RocksDB (grocksdb)

```bash
# 1. 更新 go.mod
go get github.com/linxGnu/grocksdb@latest

# 2. 运行测试（特别是 storage 测试）
make test-storage
```

## 安全更新

定期检查依赖的安全漏洞：

```bash
# 使用 govulncheck
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...

# 或使用 nancy (检查 go.mod)
go list -json -m all | docker run --rm -i sonatypecommunity/nancy:latest sleuth
```

## 获取依赖信息

```bash
# 查看所有依赖
go mod graph

# 查看依赖树
go mod graph | grep -v indirect

# 更新所有依赖到最新兼容版本
go get -u ./...
go mod tidy

# 查看可升级的依赖
go list -u -m all
```

## 锁定依赖版本

项目使用 `go.mod` 和 `go.sum` 锁定依赖版本，确保构建可重现。

**重要**: 不要手动修改 `go.sum`，使用 `go mod tidy` 自动维护。

## 参考链接

- [Go Modules Reference](https://go.dev/ref/mod)
- [etcd Documentation](https://etcd.io/docs/)
- [gRPC Go Quick Start](https://grpc.io/docs/languages/go/quickstart/)
- [RocksDB Documentation](https://github.com/facebook/rocksdb/wiki)
- [grocksdb Repository](https://github.com/linxGnu/grocksdb)
