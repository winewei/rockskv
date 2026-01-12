# RocksKV 集成测试计划

## 目标

创建全面的集成测试套件，覆盖所有关键分布式交互场景。

## 测试架构

```
Integration Tests
├── Storage Layer (已完成 ✅)
│   ├── node_lease_integration_test.go  ✅ 节点租约、KeepAlive
│   ├── primary_check_test.go           ✅ 主副本检查
│   └── epoch_fencing_test.go           ✅ Epoch 防护
│
├── Metadata Layer (已完成 ✅)
│   ├── ha_integration_test.go          ✅ Leader 选举、failover (7个测试)
│   ├── route_table_integration_test.go ✅ 路由表持久化、订阅 (5个测试)
│   └── node_registration_integration_test.go ✅ 节点注册、心跳 (5个测试)
│
├── Migration (部分完成 ⚠️)
│   └── migration_integration_test.go   ⚠️ SST 导出/导入测试框架 (2个待修复, 3个待实现)
│
├── Compute-Storage (框架完成 ⚠️)
│   └── compute_storage_integration_test.go ⚠️ 测试框架 (4个待实现)
│
└── End-to-End (框架完成 ⚠️)
    └── e2e_integration_test.go         ⚠️ 测试框架 (5个待实现)
```

## 详细测试场景

### 1. Metadata HA 集成测试 (ha_integration_test.go) ✅

**状态**: ✅ 已完成 (2026-01-11)

**测试用例**：
- [x] TestLeaderElection：3 个 Metadata 节点，验证 Leader 选举
- [x] TestLeaderFailover：Kill Leader，验证自动切换
- [x] TestWriteForwarding：非 Leader 节点拒绝写操作
- [x] TestReadScaling：所有节点可以处理读操作
- [x] TestGetLeaderInfo：客户端可以发现当前 Leader
- [x] TestSingleNodeMode：单节点模式（ha_enabled=false）
- [x] TestHeartbeatOnAllNodes：心跳在所有节点工作

**测试结果**：
- 7 个测试全部通过
- 执行时间：~31 秒
- 覆盖场景：leader 选举、failover、写转发、读扩展、客户端发现、单节点模式

**依赖**：
- etcd (localhost:2379)
- 3 个 Metadata 节点（临时端口）

---

### 2. 路由表集成测试 (route_table_integration_test.go) ✅

**状态**: ✅ 已完成 (2026-01-11)

**测试用例**：
- [x] TestRouteTablePersistence：路由表写入 etcd 持久化，验证版本自增
- [x] TestRouteTableWatch：etcd Watch API 接收路由表更新通知
- [x] TestRouteSubscription：Router.Subscribe() 机制，多订阅者
- [x] TestInitCluster：集群初始化，一致性哈希分区分配
- [x] TestPartitionReassignment：节点故障后分区重新分配（已跳过，待实现）

**测试结果**：
- 4 个测试全部通过（1 个跳过）
- 执行时间：~3.9 秒
- 覆盖场景：路由表持久化、Watch机制、订阅模式、一致性哈希、版本管理

**关键实现**：
- cleanupEtcd() 辅助函数确保测试隔离
- 版本检查改为相对值（version > 0, version = old + 1）而非硬编码
- 分区分布容差提高到 20% 以适应一致性哈希方差

**依赖**：
- etcd (localhost:2379)
- EtcdStore、Router 组件

---

### 3. 节点注册集成测试 (node_registration_integration_test.go) ✅

**状态**: ✅ 已完成 (2026-01-12)

**测试用例**：
- [x] TestNodeRegistration：Storage 节点注册到 Metadata，验证节点信息持久化
- [x] TestHeartbeat：心跳机制保持节点状态，验证 TTL lease
- [x] TestNodeTimeout：心跳停止后节点标记为 OFFLINE，failover 自动触发
- [x] TestNodeRejoin：OFFLINE 节点重新发送心跳后恢复
- [x] TestNodeStatusTransitions：节点状态转换（online → draining → offline → removed）

**测试结果**：
- 5 个测试全部通过
- 执行时间：~3.5 秒
- 覆盖场景：节点注册、心跳 TTL、failover、节点恢复、状态转换

**关键实现**：
- 直接测试 Store 层而不是 gRPC Server 层，简化测试
- 使用 FailoverManager 验证心跳超时和故障转移
- 使用 cleanupEtcdForNodeTests() 确保测试隔离
- 验证 etcd 中的心跳键使用 15 秒 TTL lease

**依赖**：
- etcd (localhost:2379)
- EtcdStore、Router、FailoverManager 组件

---

### 4. 分区迁移集成测试 (migration_integration_test.go) ⚠️

**状态**: ⚠️ 部分完成 (2026-01-12)

**测试用例**：
- [x] TestSSTExport：从源节点导出 SST 文件 ✅ 已修复通过
- [ ] TestSSTImport：目标节点导入 SST 文件 (已跳过 - 待更新)
- [ ] TestMigrationTriggerRebalance：触发重平衡迁移 (已跳过 - 需要 MigrationController)
- [ ] TestMigrationCancel：取消进行中的迁移 (已跳过 - 待实现)
- [ ] TestConcurrentMigrations：并发迁移多个分区 (已跳过 - 待实现)

**测试结果**：
- 1 个测试通过，4 个测试跳过
- 执行时间：~45 秒
- 覆盖场景：SST 导出功能验证

**已修复问题** (2026-01-12)：
1. **Epoch 字段缺失**：`GetRouteTable` 和 `sendRouteUpdate` 返回的分区信息缺少 Epoch 字段
2. **MinReplicaNodes 要求**：测试需要至少 2 个存储节点才能初始化集群
3. **时序问题**：改为让 Storage 节点自动注册，而非测试代码手动注册

**依赖**：
- etcd (localhost:2379)
- Metadata Server (非 HA 模式)
- 2 个 Storage 节点

**下一步**：
- 更新 TestSSTImport 测试使用相同模式
- 实现 MigrationController 相关测试
- 实现迁移取消和并发迁移测试

---

### 5. Compute-Storage 集成测试 (compute_storage_integration_test.go) ⚠️

**状态**: ⚠️ 框架完成 (2026-01-12)

**测试用例**：
- [ ] TestStorageConnectionPool：存储节点连接池管理 (已跳过 - 待实现)
- [ ] TestPrimaryReplicaSync：主副本同步写验证 (已跳过 - 待实现)
- [ ] TestStorageNodeFailover：存储节点故障重试 (已跳过 - 待实现)
- [ ] TestRouteTableUpdatePropagation：路由表更新传播 (已跳过 - 待实现)

**测试结果**：
- 4 个测试全部跳过 (Skipped)
- 测试框架已搭建，包含详细实现计划

**推迟原因**：
1. 依赖 Migration 测试的分区分配问题先解决
2. 需要协调 Metadata + Storage + Compute 三层，复杂度高
3. 建议先通过单元测试验证各层交互逻辑

**依赖**：
- etcd (localhost:2379)
- 1 个 Metadata 节点
- 2 个 Storage 节点
- 1 个 Compute 节点

**下一步**：
- 修复 Migration 测试的分区分配问题
- 参考 Migration 测试的模式实现 Compute-Storage 交互
- 或优先通过单元测试 + mock 验证交互逻辑

---

### 6. End-to-End 集成测试 (e2e_integration_test.go) ⚠️

**状态**: ⚠️ 框架完成 (2026-01-12)

**测试用例**：
- [ ] TestFullStackPutGet：SDK → Compute → Storage → RocksDB 全栈测试 (已跳过)
- [ ] TestCrossPartitionBatch：跨分区批量操作 (已跳过)
- [ ] TestComputeLoadBalance：多 Compute 节点负载均衡 (已跳过)
- [ ] TestStorageFailoverE2E：存储层故障对客户端的影响 (已跳过)
- [ ] TestMetadataFailoverE2E：元数据层故障对服务的影响 (已跳过)

**测试结果**：
- 5 个测试全部跳过 (Skipped)
- 测试框架已搭建，包含详细实现计划和现有 smoke test 说明

**推迟原因**：
1. 最复杂的集成场景，需要所有下层测试先稳定
2. 需要完整多节点集群：2 Metadata + 3 Storage + 2 Compute
3. 现有 scripts/smoke-test.sh 已提供基本 E2E 验证

**当前 E2E 覆盖**：
- ✅ Docker Compose 全栈部署测试 (docker-compose.yml)
- ✅ Shell 脚本烟雾测试 (scripts/smoke-test.sh)
- ✅ CLI 基本操作验证 (Put/Get/Delete)

**依赖**：
- etcd
- 2 个 Metadata 节点 (HA 模式)
- 3 个 Storage 节点
- 2 个 Compute 节点
- Go SDK Client

**下一步**：
- 优先修复 Migration、Compute-Storage 测试
- 考虑将 smoke-test.sh 转换为 Go 测试
- 实施 Docker-based 集成测试环境

---

### 7. 原计划：分区迁移集成测试 (已在上文实现为 Section 4)

**测试用例**：
- [ ] TestSSTExport：从源节点导出 SST 文件
- [ ] TestSSTImport：目标节点导入 SST 文件
- [ ] TestMigrationStateMachine：PENDING → EXPORTING → IMPORTING → COMPLETED
- [ ] TestMigrationCancel：取消进行中的迁移
- [ ] TestMigrationFailureRollback：迁移失败后回滚
- [ ] TestConcurrentMigrations：多个分区并发迁移

**依赖**：
- etcd
- 1 个 Metadata 节点
- 2 个 Storage 节点（源和目标）

---

### 8. 原计划：Compute-Storage 集成测试 (已在上文实现为 Section 5)

**测试用例**：
- [ ] TestStorageConnectionPool：连接池创建和复用
- [ ] TestPrimaryReplicaSync：主副本同步写，both succeed
- [ ] TestPrimaryWriteFailure：主副本写入失败处理
- [ ] TestStorageNodeFailover：Storage 节点故障时重试
- [ ] TestRouteTableUpdate：Compute 接收路由更新后更新连接

**依赖**：
- etcd
- 1 个 Metadata 节点
- 2 个 Storage 节点
- 1 个 Compute 节点

---

### 6. 端到端集成测试 (e2e_integration_test.go)

**测试用例**：
- [ ] TestFullStackPutGet：Client → Compute → Storage → RocksDB
- [ ] TestCrossPartitionBatch：批量操作跨多个分区
- [ ] TestComputeLoadBalance：多个 Compute 节点负载均衡
- [ ] TestStorageFailoverE2E：Storage 节点故障后客户端重试成功
- [ ] TestMetadataFailoverE2E：Metadata Leader 切换后服务正常

**依赖**：
- etcd
- 2 个 Metadata 节点
- 3 个 Storage 节点
- 2 个 Compute 节点
- Go SDK Client

---

## 测试工具类

### TestCluster Helper (test/testcluster/cluster.go)

```go
type TestCluster struct {
    EtcdURL      string
    Metadatas    []*metadata.Server
    Storages     []*storage.Server
    Computes     []*compute.Server
}

func NewTestCluster(opts ...Option) (*TestCluster, error)
func (tc *TestCluster) Cleanup()
func (tc *TestCluster) AddStorageNode() (*storage.Server, error)
func (tc *TestCluster) KillMetadataLeader() error
```

---

## Makefile 更新

```makefile
# 运行所有集成测试
test-integration:
	@echo "Starting etcd for integration tests..."
	@docker-compose up -d etcd
	@sleep 2
	@echo "Running integration tests..."
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration -timeout 5m \
		./pkg/storage/... \
		./pkg/metadata/... \
		./pkg/compute/... \
		./test/integration/... || (make test-integration-cleanup && exit 1)
	@make test-integration-cleanup

# 分别运行各层集成测试
test-integration-storage:
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration ./pkg/storage/...

test-integration-metadata:
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration ./pkg/metadata/...

test-integration-compute:
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration ./pkg/compute/...

test-integration-e2e:
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration ./test/integration/...
```

---

## 实施计划

### Phase 1：基础设施 ✅ (部分完成)
- [ ] 创建 test/testcluster 包（TestCluster helper） - 待实施
- [x] 更新 Makefile 支持多包集成测试 - ✅ 已完成

### Phase 2：Metadata 测试 ✅ (已完成)
- [x] ha_integration_test.go - ✅ 已完成 (7个测试)
- [x] route_table_integration_test.go - ✅ 已完成 (5个测试，1个跳过)
- [x] node_registration_integration_test.go - ✅ 已完成 (5个测试)

### Phase 3：Migration 测试 ⚠️ (部分完成)
- [x] migration_integration_test.go - ⚠️ 框架已搭建，5个测试跳过（分区分配问题待解决）

### Phase 4：Compute-Storage 测试 ⚠️ (框架完成)
- [x] compute_storage_integration_test.go - ⚠️ 框架已搭建，4个测试跳过（待实现）

### Phase 5：E2E 测试 ⚠️ (框架完成)
- [x] test/integration/e2e_integration_test.go - ⚠️ 框架已搭建，5个测试跳过（待实现）

---

## 当前进度

**已完成**：
- ✅ Storage Layer: 3 个测试文件，7 个集成测试
- ✅ Metadata HA: 1 个测试文件，7 个集成测试
- ✅ Metadata Route Table: 1 个测试文件，5 个集成测试（4个通过，1个跳过）
- ✅ Metadata Node Registration: 1 个测试文件，5 个集成测试
- ⚠️ Migration: 1 个测试文件，5 个测试（1个通过，4个跳过）
- ⚠️ Compute-Storage: 1 个测试文件，4 个测试（全部跳过 - 待实现）
- ⚠️ E2E: 1 个测试文件，5 个测试（全部跳过 - 待实现）
- ✅ Makefile 更新：支持 ./pkg/storage/... 和 ./pkg/metadata/... 和 ./test/integration/...
- ✅ etcd 启动脚本优化：统一 PID 管理，fail-fast 连接验证

**测试统计**：
- 总测试文件：9 个
- 总测试用例：38 个（24个通过，14个跳过）
- 执行时间：~90 秒（通过的测试，包含 etcd 启动和清理）
- 框架完成度：100%（所有 5 个阶段的测试框架已搭建）
- 实现完成度：63%（24/38 测试实现并通过）

**测试框架已搭建但待实现**：
- ⚠️ 分区迁移集成测试 - 4个测试跳过（TestSSTExport 已通过）
- ⚠️ Compute-Storage 集成测试 - 4个测试跳过（待实现）
- ⚠️ E2E 集成测试 - 5个测试跳过（最复杂场景，现有 smoke-test.sh 提供基本覆盖）

**关键里程碑**：
- ✅ 所有 5 个测试阶段的框架已完成
- ✅ Storage 和 Metadata 层测试全部通过
- ✅ Migration TestSSTExport 已修复并通过（2026-01-12）
- ✅ 每个跳过的测试都包含详细的TODO和实现计划

**进度**：38 个测试框架完成 / 24 个测试实现通过 (框架 100% 完成，实现 63% 完成)

---

## 预期效果

- **测试覆盖**：从 3 个集成测试 → 30+ 个集成测试
- **测试时间**：预计 30-60 秒（并行运行）
- **CI 集成**：GitHub Actions 自动运行
- **置信度**：验证所有关键分布式交互场景
