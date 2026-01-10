# RocksKV 改进计划

## 问题1: 路由表更新并发冲突

### 现状
- MigrationController 并发迁移多个分区（MaxConcurrent=10）
- 每个分区迁移完成后独立更新路由表
- 使用 etcd CAS 操作，多个 goroutine 同时更新导致版本冲突
- 当前重试机制（5次）在高并发下仍会失败

### 解决方案：etcd 分布式锁

使用 etcd 的 `concurrency` 包实现分布式锁，保证路由表更新的强一致性。

#### 实现步骤

1. **添加依赖**
   - `go.etcd.io/etcd/client/v3/concurrency` 已包含在 etcd client 中

2. **修改 `pkg/metadata/etcd.go`**
   - 在 EtcdStore 中添加 session 和 mutex
   - 创建路由表更新锁：`/rockskv/locks/route-table`

3. **修改 `UpdateRouteTable` 方法**
   ```go
   func (s *EtcdStore) UpdateRouteTable(ctx context.Context, table *RouteTable) error {
       // 获取分布式锁
       if err := s.routeTableMutex.Lock(ctx); err != nil {
           return fmt.Errorf("failed to acquire route table lock: %w", err)
       }
       defer s.routeTableMutex.Unlock(ctx)

       // 在锁保护下读取最新版本
       current, err := s.GetRouteTable(ctx)
       if err != nil {
           return err
       }

       // 更新版本号
       table.Version = current.Version + 1

       // 写入 etcd
       return s.putRouteTable(ctx, table)
   }
   ```

4. **修改 `pkg/metadata/router.go`**
   - `CompleteMigration` 等方法无需再处理版本冲突重试
   - 锁已保证串行化

5. **Session 管理**
   - 使用 TTL=30s 的租约
   - 添加 keepalive 保持 session 活跃
   - 在 Close() 中正确释放 session

#### 涉及文件
- `pkg/metadata/etcd.go` - 添加分布式锁
- `pkg/metadata/router.go` - 简化更新逻辑
- `pkg/metadata/migration.go` - 移除重试逻辑

---

## 问题2: 分区分配时机

### 现状
- 第 2 个存储节点注册时自动触发分区初始化
- 后续节点注册时分区已分配完毕
- 导致分区分布不均，需要大量 rebalance

### 解决方案：两阶段初始化（方案D）

阶段1: 集群启动 → 进入 "pending" 状态 → 接受节点注册但不分配分区
阶段2: 管理员执行 init-cluster 或 自动检测稳定后初始化

---

## 分区处理边界条件

### 边界1: 集群生命周期阶段

| 阶段 | 描述 | 读写请求处理 | 决策 |
|------|------|--------------|------|
| **PENDING** | 集群刚启动，无分区表 | 拒绝读写，返回 `CLUSTER_NOT_INITIALIZED` | ✅ 已确定 |
| **INITIALIZING** | 正在分配分区 | 返回 `CLUSTER_INITIALIZING`，客户端重试 | ✅ 已确定 |
| **RUNNING** | 分区已分配，服务正常 | 正常处理（个别分区可能降级，见下方说明） | ✅ 已确定 |

**注意：** MAINTENANCE 和 DEGRADED 不是独立的集群状态：
- **MAINTENANCE（rebalance/迁移）**：RUNNING 状态下的子操作，不影响集群状态
- **DEGRADED（降级）**：分区级别状态，不是集群级别状态

**状态转换图：**
```
PENDING ──(init-cluster)──> INITIALIZING ──(完成)──> RUNNING
```

**注意：** DEGRADED 不是集群状态，而是分区级别的状态。集群始终处于 RUNNING，但个别分区可能处于降级状态。

**分区降级处理（Partition-Level Degraded）：**
- 判断条件：分区的可用副本数 < 配置副本数
- 行为：
  - **分区级别判断，不阻断整个集群**
  - 至少 1 个副本可用 → 正常读写 + 响应带 warning 标记 + metrics 计数
  - 0 个副本可用 → 该分区拒绝读写，返回 `PARTITION_UNAVAILABLE`
- 指标：`partition_degraded_count`, `partition_unavailable_count`

---

### 边界2: 节点状态变化

| 事件 | 行为 | 决策 |
|------|------|------|
| **新节点注册** | 只注册不分配分区，等待 rebalance（手动或自动，可配置） | ✅ 已确定 |
| **节点主动下线** | Controlled Shutdown：先迁移 Primary 到 Replica，再下线（参考 Kafka） | ✅ 已确定 |
| **节点心跳超时** | 心跳 100ms，超时 1s（10次），参考 etcd | ✅ 已确定 |
| **节点恢复上线** | 视为新节点，需重新同步数据（支持增量同步如有点位） | ✅ 已确定 |
| **节点永久移除** | 必须先 drain（迁移完所有分区）才能移除 | ✅ 已确定 |

**节点心跳机制（Storage → Metadata）：**
```
心跳间隔: 100ms
超时判定: 1s (10次心跳丢失)
用途: 检测节点存活状态

正常 ──(1s无心跳)──> OFFLINE ──> 触发 failover
  ↑                    |
  └──(收到心跳)────────┘ (如已 failover，视为新节点处理)
```

**节点状态：**
- `ONLINE`: 正常运行
- `OFFLINE`: 心跳超时，触发 failover
- `DRAINING`: 正在迁移分区，准备下线
- `REMOVED`: 已从集群移除

**Controlled Shutdown 流程（参考 Kafka）：**
```
管理员执行 shutdown-node <node_id>
         │
         ▼
    节点标记 DRAINING
         │
         ▼
    该节点上所有 Primary 分区
    执行 leader 切换到 Replica
         │
         ▼
    等待切换完成（或超时）
         │
         ▼
    节点标记 OFFLINE，停止服务
```

**主动下线 vs 永久移除：**
| 操作 | 命令 | 行为 | 数据处理 |
|------|------|------|----------|
| 主动下线 | `shutdown-node` | Controlled Shutdown | Primary 切换，数据保留在节点 |
| 永久移除 | `remove-node` | Drain + Remove | 迁移所有数据，从集群移除 |

---

### 边界3: 分区状态机

| 状态 | 允许读 | 允许写 | 说明 | 决策 |
|------|--------|--------|------|------|
| **NORMAL** | ✅ | ✅ | 正常运行 | ✅ 已确定 |
| **MIGRATING_OUT** | ✅ | ✅ | 读写继续，迁移完成后追赶增量数据再切换 | ✅ 已确定 |
| **MIGRATING_IN** | ❌ | ❌ | 迁移完成前不接受请求，路由仍指向源节点 | ✅ 已确定 |
| **OFFLINE** | ❌ | ❌ | 返回 `PARTITION_UNAVAILABLE`，需人工确认后恢复 | ✅ 已确定 |

**状态转换图：**
```
┌──────────┐
│ CREATING │ (初始化时)
└────┬─────┘
     │
     ▼
┌──────────┐   开始迁移(源)   ┌───────────────┐   迁移完成    ┌─────────┐
│  NORMAL  │ ───────────────> │ MIGRATING_OUT │ ────────────> │ DELETED │
└────┬─────┘                  └───────────────┘               └─────────┘
     │
     │ 开始迁移(目标)   ┌──────────────┐   迁移完成   ┌─────────┐
     └───────────────> │ MIGRATING_IN │ ──────────> │ NORMAL  │
                       └──────────────┘             └─────────┘
     │
     │ 双副本都故障
     ▼
┌──────────┐   人工确认恢复   ┌─────────┐
│ OFFLINE  │ ───────────────> │ NORMAL  │
└──────────┘                  └─────────┘
```

**MIGRATING_OUT 增量同步机制：**
- 迁移期间源节点正常接受读写
- 全量数据传输完成后，进入 catchup 阶段
- 追赶迁移期间的增量写入（基于 WAL 或时间戳）
- 增量追平后，原子切换路由表，源节点标记删除

**OFFLINE 恢复流程：**
1. 人工检查数据完整性
2. 执行 `recover-partition <partition_id>` 命令
3. 如需从备份恢复，执行 `restore-partition <partition_id> <backup_path>`

---

### 边界4: 副本策略

| 问题 | 决策 | 说明 |
|------|------|------|
| **副本数** | 默认 2 副本，可配置 | `replica_count` 配置项 | ✅ 已确定 |
| **写入策略** | 异步复制 | Primary 写入成功即返回，异步复制到 Replica | ✅ 已确定 |
| **读取策略** | 可配置 (`read_concern: primary/any`) | 默认 primary | ✅ 已确定 |
| **Primary 故障** | 自动提升，提升后立即可写 | 需监控同步 lag | ✅ 已确定 |
| **Replica 故障** | 延迟补充（默认 10 分钟） | 读路由需更新 | ✅ 已确定 |

**异步复制流程：**
```
Client ──写入──> Primary ──返回成功──> Client
                    │
                    └──异步──> Replica
```

**异步复制的 Trade-off：**
- **优点**：低延迟，高吞吐
- **风险**：Primary 故障时可能丢失未同步数据
- **应对措施**：
  - 监控 `replica_sync_lag_bytes` 和 `replica_sync_lag_seconds`
  - 设置告警阈值，lag 过大时及时处理
  - 关键业务可考虑使用同步写入（未来扩展）

**同步点位机制：**
- 每个分区维护 `sync_offset`（已确认同步的位置）
- Primary 写入后记录 WAL，生成单调递增的 offset
- Replica 同步后确认 offset，Primary 更新 `sync_offset`
- Primary 故障时，`sync_offset` 之后的数据可能丢失（WAL 在故障节点上）

```
WAL: [offset=100] [offset=101] [offset=102] [offset=103]
                                    ↑
                              sync_offset=102
                              (Replica 已确认到 102)

Primary 故障后：
- offset=103 数据可能丢失（在故障节点的 WAL 中）
- Replica 提升为 Primary，从 offset=102 继续
```

**监控指标：**
- `replica_sync_lag_bytes`: Replica 同步延迟（字节）
- `replica_sync_lag_seconds`: Replica 同步延迟（时间）
- `replica_sync_offset`: 当前同步点位

**Replica 故障处理：**
1. 检测到 Replica 故障
2. 如果 `read_concern=any`，更新路由将读请求转发到 Primary
3. 等待 10 分钟（可配置）
4. 如未恢复，在其他节点创建新 Replica 并同步数据
5. 同步完成后更新路由表

---

### 边界5: 故障处理

| 场景 | 决策 | 说明 |
|------|------|------|
| **Primary 故障** | 直接提升 Replica，不等待确认 | 可能丢少量未同步数据 | ✅ 已确定 |
| **Replica 故障** | 写入返回 warning，提示降级运行 | 不阻塞写入 | ✅ 已确定 |
| **双副本都故障** | 分区 OFFLINE，人工确认后恢复 | 接受数据丢失风险 | ✅ 已确定 |
| **脑裂检测** | Lease 机制，Primary 需持续续约 | 依赖 etcd | ✅ 已确定 |
| **脑裂恢复** | 以新 Primary 为准，旧 Primary 数据丢弃 | | ✅ 已确定 |
| **数据不一致** | 定期 checksum 校验，以 Primary 为准重建 Replica | | ✅ 已确定 |

**Primary 故障 Failover 流程：**
```
Primary 心跳超时 (1s)
         │
         ▼
    检查 Replica 状态
         │
    ┌────┴────┐
    │         │
 正常      也故障
    │         │
    ▼         ▼
提升为     分区标记
新Primary  OFFLINE
    │
    ▼
更新路由表
    │
    ▼
立即可写（可能丢失未同步数据）
```

**Replica 故障期间写入响应：**
```json
{
  "success": true,
  "warning": "DEGRADED_MODE: partition running with single replica",
  "partition_id": 123
}
```

**分区 Lease 防脑裂机制（独立于节点心跳）：**

| 机制 | 用途 | 参数 |
|------|------|------|
| 节点心跳 | Storage → Metadata，检测节点存活 | 100ms 间隔，1s 超时 |
| 分区 Lease | Primary 在 etcd 上的租约，防脑裂 | TTL=1s，100ms 续约 |

**Lease 工作流程：**
- Primary 启动时从 etcd 获取分区 Lease（TTL=1s）
- Primary 持续续约（每 100ms）
- 如果无法续约（网络分区），Lease 过期
- Lease 过期后，Primary 自动降级，停止接受写入
- Replica 检测到 Primary Lease 过期后，竞争新 Lease 成为 Primary

```
Primary A ──持有 Lease──> 网络分区 ──> Lease 过期 ──> 停止写入
                              │
Replica B ──检测 Lease 过期──> 获取新 Lease ──> 成为新 Primary
```

**两个机制的协作：**
1. 节点心跳超时 → Metadata 标记节点 OFFLINE → 触发该节点所有分区的 failover
2. 分区 Lease 过期 → Primary 自动停写 → Replica 竞争成为新 Primary
3. 正常情况下节点心跳和分区 Lease 同时工作，提供双重保障

**数据一致性校验：**
- 校验频率：每小时（可配置）
- 校验方式：分区级 checksum 比对
- 不一致处理：
  1. 记录告警日志
  2. 上报指标 `partition_checksum_mismatch`
  3. 触发 Replica 重建任务

---

### 边界6: 初始化具体问题

| 问题 | 决策 | 说明 |
|------|------|------|
| **最小节点数** | min_nodes = 副本数 | 2 副本需 2 节点，3 副本需 3 节点 | ✅ 已确定 |
| **自动初始化** | 可配置 `auto_init: true/false` | 默认 false | ✅ 已确定 |
| **自动初始化条件** | 节点数 >= min_nodes + 稳定期 60s | | ✅ 已确定 |
| **手动初始化检查** | 检查失败则拒绝执行，返回具体错误 | | ✅ 已确定 |
| **初始化失败处理** | 自动回滚到 PENDING 状态，允许重试 | | ✅ 已确定 |
| **初始化中节点故障** | 中止初始化，回滚，等待恢复后重试 | | ✅ 已确定 |
| **分配算法** | 一致性哈希 | | ✅ 已确定 |

**配置示例：**
```yaml
cluster:
  replica_count: 2              # 副本数（含 Primary）
  auto_init: false              # 是否自动初始化
  auto_init_stable_period: 60s  # 自动初始化稳定期
  dev_mode: false               # 开发模式（允许单节点单副本）
```

**开发测试模式 (dev_mode: true)：**
- 允许单节点部署
- 允许单副本（无 Replica）
- 跳过部分检查
- **生产环境禁止开启**

**手动初始化前置检查：**
```
init-cluster 执行前检查：
  [1] 集群状态 == PENDING          ✓/✗
  [2] 存储节点数 >= min_nodes      ✓/✗ (当前: N, 需要: M)
  [3] 所有节点心跳正常              ✓/✗ (异常节点: xxx)
  [4] etcd 连接正常                ✓/✗
  [5] 无进行中的其他操作            ✓/✗

任一检查失败则拒绝执行，显示具体原因。
```

**初始化流程：**
```
PENDING
   │
   ▼
执行 init-cluster
   │
   ├──(检查失败)──> 返回错误，保持 PENDING
   │
   ▼
INITIALIZING
   │
   ├──(节点故障)──> 回滚到 PENDING
   ├──(etcd失败)──> 回滚到 PENDING
   │
   ▼
使用一致性哈希分配 4096 分区
   │
   ▼
写入路由表到 etcd
   │
   ▼
RUNNING
```

**一致性哈希分配算法：**
```
1. 为每个节点在哈希环上创建多个虚拟节点（如 100 个）
2. 对每个分区 ID 计算哈希值，顺时针找到第一个虚拟节点作为 Primary
3. 继续顺时针找到下一个不同物理节点作为 Replica
4. 结果：
   - 分区均匀分布
   - 扩容时只需迁移部分分区
   - Primary 和 Replica 保证在不同节点
```

---

### 边界7: Rebalance 边界

| 问题 | 决策 | 说明 |
|------|------|------|
| **触发条件** | 可配置自动触发 | 节点加入后 N 分钟自动触发 | ✅ 已确定 |
| **并发度** | 可配置 | 默认 10 | ✅ 已确定 |
| **带宽限制** | 可配置 | 默认 100MB/s per migration | ✅ 已确定 |
| **迁移优先级** | 可配置策略 | 默认按负载 | ✅ 已确定 |
| **中断处理** | 已完成保留，进行中回滚，剩余取消 | | ✅ 已确定 |
| **恢复方式** | 支持 `rebalance` 重新计算 + `resume-rebalance` 继续 | | ✅ 已确定 |
| **目标均衡度** | 负载均衡（考虑热点分裂） | 差异 ≤10% 不自动迁移 | ✅ 已确定 |
| **源节点故障** | 迁移失败，标记等待重试 | | ✅ 已确定 |
| **目标节点故障** | 迁移失败，选择新目标节点重试 | | ✅ 已确定 |
| **Metadata 故障** | 迁移暂停，恢复后自动继续 | | ✅ 已确定 |

**配置示例：**
```yaml
rebalance:
  auto_trigger: true                    # 是否自动触发
  auto_trigger_delay: 300s              # 节点变化后延迟触发时间
  max_concurrent_migrations: 10         # 最大并发迁移数
  max_migrations_per_node: 5            # 每节点最大同时迁移数
  bandwidth_limit_per_migration: 100MB  # 每个迁移的带宽限制
  balance_threshold: 0.1                # 均衡阈值（10%差异内不迁移）
  priority_strategy: load               # 优先级策略: partition_id/size/load
```

**优先级策略：**
| 策略 | 说明 |
|------|------|
| `partition_id` | 按分区 ID 顺序 |
| `size_asc` | 先迁移小分区 |
| `size_desc` | 先迁移大分区 |
| `load` | 先迁移高负载分区（分散热点） |

**均衡度计算：**
```
节点负载 = 数据量权重 * 0.4 + QPS权重 * 0.6

不均衡度 = (max(节点负载) - min(节点负载)) / avg(节点负载)

if 不均衡度 > 10%:
    触发 rebalance 告警
    if auto_trigger:
        延迟 auto_trigger_delay 后自动执行
```

**热点分裂：**
- 检测单分区 QPS > 阈值时，标记为热点
- 热点分区优先迁移到负载低的节点
- 未来可考虑分区分裂（超出当前范围）

**中断与恢复：**
```
rebalance 运行中
       │
       ├──(cancel-rebalance)──> 已完成的保留
       │                        进行中的回滚
       │                        剩余的取消
       │                        状态记录到 etcd
       │
       └──(故障中断)──────────> 同上，恢复后可执行：
                                 - rebalance: 重新计算迁移计划
                                 - resume-rebalance: 继续未完成的
```

**迁移故障处理流程：**
```
迁移分区 P: NodeA -> NodeB
              │
         ┌────┴────┐
         │         │
     NodeA故障  NodeB故障
         │         │
         ▼         ▼
   标记P迁移    选择NodeC
   失败,等待    作为新目标
   NodeA恢复    重新迁移
   后重试       P: NodeA -> NodeC
```

