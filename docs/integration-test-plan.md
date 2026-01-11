# RocksKV 集成测试计划

## 目标

创建全面的集成测试套件，覆盖所有关键分布式交互场景。

## 测试架构

```
Integration Tests
├── Storage Layer (已完成)
│   ├── node_lease_integration_test.go  ✅
│   ├── primary_check_test.go           ✅
│   └── epoch_fencing_test.go           ✅
│
├── Metadata Layer (待补充)
│   ├── ha_integration_test.go          ❌ Leader 选举、failover
│   ├── route_table_integration_test.go ❌ 路由表持久化、订阅
│   └── node_registration_test.go       ❌ 节点注册、心跳
│
├── Migration (待补充)
│   └── migration_integration_test.go   ❌ SST 导出/导入、状态机
│
├── Compute-Storage (待补充)
│   └── compute_storage_integration_test.go ❌ 连接池、同步写、重试
│
└── End-to-End (待补充)
    └── e2e_integration_test.go         ❌ Client → Compute → Storage
```

## 详细测试场景

### 1. Metadata HA 集成测试 (ha_integration_test.go)

**测试用例**：
- [ ] TestLeaderElection：3 个 Metadata 节点，验证 Leader 选举
- [ ] TestLeaderFailover：Kill Leader，验证自动切换
- [ ] TestWriteForwarding：非 Leader 节点拒绝写操作
- [ ] TestReadScaling：所有节点可以处理读操作
- [ ] TestGetLeaderInfo：客户端可以发现当前 Leader

**依赖**：
- etcd (localhost:2379)
- 3 个 Metadata 节点（临时端口）

---

### 2. 路由表集成测试 (route_table_integration_test.go)

**测试用例**：
- [ ] TestRouteTablePersistence：路由表写入 etcd 持久化
- [ ] TestRouteSubscription：Storage 节点订阅路由更新
- [ ] TestRouteUpdate：InitCluster 后路由表正确生成
- [ ] TestPartitionReassignment：节点故障后分区重新分配

**依赖**：
- etcd
- 1 个 Metadata 节点
- 2 个 Storage 节点

---

### 3. 节点注册集成测试 (node_registration_test.go)

**测试用例**：
- [ ] TestNodeRegistration：Storage 节点注册到 Metadata
- [ ] TestHeartbeat：心跳保持节点状态
- [ ] TestNodeTimeout：心跳停止后节点标记为 DOWN
- [ ] TestNodeRejoin：DOWN 节点重新加入

**依赖**：
- etcd
- 1 个 Metadata 节点
- 1 个 Storage 节点

---

### 4. 分区迁移集成测试 (migration_integration_test.go)

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

### 5. Compute-Storage 集成测试 (compute_storage_integration_test.go)

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

### Phase 1：基础设施 (1-2 天)
- [ ] 创建 test/testcluster 包（TestCluster helper）
- [ ] 更新 Makefile 支持多包集成测试

### Phase 2：Metadata 测试 (2-3 天)
- [ ] ha_integration_test.go
- [ ] route_table_integration_test.go
- [ ] node_registration_test.go

### Phase 3：Migration 测试 (2-3 天)
- [ ] migration_integration_test.go

### Phase 4：Compute 测试 (2-3 天)
- [ ] compute_storage_integration_test.go

### Phase 5：E2E 测试 (2-3 天)
- [ ] e2e_integration_test.go

---

## 预期效果

- **测试覆盖**：从 3 个集成测试 → 30+ 个集成测试
- **测试时间**：预计 30-60 秒（并行运行）
- **CI 集成**：GitHub Actions 自动运行
- **置信度**：验证所有关键分布式交互场景
