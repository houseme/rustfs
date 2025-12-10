# EC法定人数管理系统

## 概述

EC（纠删码）法定人数管理系统提供自动磁盘消除和写入操作验证，确保在磁盘故障时维护数据完整性和可用性。

## 核心概念

### 纠删码配置

RustFS使用纠删码（Erasure Coding）来提供数据冗余和容错能力：

- **D (data_shards)**: 数据分片数量
- **P (parity_shards)**: 奇偶校验分片数量
- **N (total)**: 总磁盘数 = D + P

### 法定人数规则

为确保数据完整性和可恢复性，系统定义了以下法定人数要求：

- **写入法定人数**: `D + 1` 个磁盘必须可用
- **读取法定人数**: `D` 个磁盘必须可用

**为什么写入需要D+1？**

写入操作需要D+1个磁盘以确保：
1. 至少有足够的分片来重建数据（D个数据分片）
2. 额外的1个分片提供容错能力
3. 即使在写入过程中有一个磁盘失败，仍能保持数据完整性

## 架构设计

### QuorumManager

核心组件，负责管理磁盘健康状态和法定人数验证：

```rust
pub struct QuorumManager {
    // 配置
    config: QuorumConfig,
    
    // 磁盘健康追踪
    disks: Vec<Arc<DiskHealth>>,
    
    // 写入暂停标志
    write_paused: AtomicBool,
    
    // 指标
    metrics_write_ops: AtomicU64,
    metrics_write_blocked: AtomicU64,
    metrics_disks_eliminated: AtomicU64,
}
```

### DiskHealth

追踪单个磁盘的健康状态：

```rust
pub struct DiskHealth {
    disk_id: String,
    state: RwLock<DiskHealthState>,  // Online/Offline/Checking
    last_success: RwLock<SystemTime>,
    consecutive_failures: AtomicUsize,
}
```

### 状态转换

```
[Online] ---3次失败---> [Offline]
    ^                        |
    |                        |
    +-------成功操作----------+
```

## 集成流程

### 与电路断路器集成

QuorumManager与现有的电路断路器系统协同工作：

1. **电路断路器**检测网络/连接层故障（3-8秒）
2. **QuorumManager**管理磁盘层故障和法定人数
3. 两者通过`should_attempt_peer()`和`record_peer_*`同步

### 写入操作流程

```
1. 应用发起写入请求
   ↓
2. QuorumManager.verify_write_quorum()
   ↓
3. 检查可用磁盘数量
   ↓
4a. available >= D+1 → 继续写入
4b. available < D+1 → 阻止写入，返回错误
   ↓
5. 执行写入到可用磁盘
   ↓
6. 记录每个磁盘的结果
   ↓
7. QuorumManager.record_disk_result()
   ↓
8. 更新磁盘健康状态
   ↓
9. 检查是否需要暂停写入
```

### 故障检测和恢复

**检测阶段**（3-8秒）：

```
磁盘故障
   ↓
连接失败/超时
   ↓
电路断路器记录失败
   ↓
3次失败后打开电路
   ↓
QuorumManager检测到电路打开
   ↓
标记磁盘为Offline
   ↓
重新评估法定人数
   ↓
如果不足，暂停写入
```

**恢复阶段**（30秒后）：

```
电路断路器进入Half-Open状态
   ↓
尝试探测请求
   ↓
成功 → 电路关闭
   ↓
QuorumManager检测到恢复
   ↓
标记磁盘为Online
   ↓
重新评估法定人数
   ↓
如果满足，恢复写入
```

## 使用指南

### 创建QuorumManager

```rust
use rustfs_ecstore::quorum_manager::{QuorumManager, QuorumConfig};

// 1. 配置EC参数（示例：4+2配置）
let config = QuorumConfig {
    data_shards: 4,              // D = 4数据分片
    parity_shards: 2,            // P = 2奇偶校验分片
    disk_health_timeout_secs: 3, // 健康检查超时
};

// 2. 准备磁盘ID列表
let disk_ids = vec![
    "disk1", "disk2", "disk3", 
    "disk4", "disk5", "disk6"
].into_iter().map(String::from).collect();

// 3. 创建管理器
let manager = QuorumManager::new(config, disk_ids);
```

### 写入前验证

```rust
// 验证写入法定人数
match manager.verify_write_quorum().await {
    Ok(available_disks) => {
        info!("写入法定人数满足，可用磁盘: {}", available_disks.len());
        
        // 执行写入操作到available_disks
        for disk in available_disks {
            // 写入数据...
        }
    }
    Err(QuorumError::InsufficientDisks { available, required }) => {
        error!("写入法定人数不足: {}/{} 可用（需要: {}）", 
               available, config.total_disks(), required);
        // 返回错误给客户端
    }
}
```

### 记录操作结果

```rust
// 写入成功
manager.record_disk_result("disk1", true).await;

// 写入失败
manager.record_disk_result("disk2", false).await;

// QuorumManager会自动：
// 1. 更新磁盘健康状态
// 2. 更新电路断路器状态
// 3. 检查是否需要暂停写入
// 4. 更新监控指标
```

### 读取前验证

```rust
match manager.verify_read_quorum().await {
    Ok(available_disks) => {
        // 执行读取操作
    }
    Err(e) => {
        error!("读取法定人数不足: {}", e);
    }
}
```

### 获取统计信息

```rust
let stats = manager.get_stats().await;

println!("总磁盘数: {}", stats.total_disks);
println!("在线: {}", stats.online_disks);
println!("离线: {}", stats.offline_disks);
println!("写入暂停: {}", stats.write_paused);
println!("总写入操作: {}", stats.total_write_ops);
println!("阻止的写入: {}", stats.write_blocked_ops);
```

### 定期磁盘消除

```rust
// 在后台任务中定期调用
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        
        let eliminated = manager.eliminate_offline_disks().await;
        if eliminated > 0 {
            warn!("消除了 {} 个离线磁盘", eliminated);
        }
    }
});
```

## 监控指标

### Prometheus指标

| 指标名称 | 类型 | 描述 | 用途 |
|---------|------|------|------|
| `quorum_disks_online_count` | Gauge | 在线磁盘数量 | 监控可用磁盘 |
| `quorum_disks_offline_count` | Gauge | 离线磁盘数量 | 故障检测 |
| `quorum_disks_checking_count` | Gauge | 检查中的磁盘数量 | 恢复过程监控 |
| `quorum_write_ops_total` | Counter | 总写入操作数 | 性能监控 |
| `quorum_write_blocked_total` | Counter | 被阻止的写入数 | 可用性监控 |
| `quorum_disks_eliminated_total` | Counter | 累计消除磁盘数 | 故障历史 |
| `quorum_write_paused_state` | Gauge | 写入暂停状态 | 实时状态 |

### 告警规则建议

```yaml
# 写入暂停告警
- alert: QuorumWritePaused
  expr: quorum_write_paused_state == 1
  for: 30s
  annotations:
    summary: "写入操作已暂停"
    description: "法定人数不足，写入操作已暂停"

# 磁盘离线告警
- alert: QuorumInsufficientDisks
  expr: quorum_disks_online_count < quorum_disks_online_count * 0.8
  for: 1m
  annotations:
    summary: "可用磁盘数量过低"
    description: "在线磁盘: {{ $value }}"

# 高写入阻止率
- alert: HighWriteBlockRate
  expr: rate(quorum_write_blocked_total[5m]) > 10
  for: 5m
  annotations:
    summary: "写入阻止率过高"
    description: "每分钟 {{ $value }} 次写入被阻止"
```

## 配置建议

### 不同场景的EC配置

| 场景 | D | P | N | 写入法定人数 | 容错能力 | 存储效率 |
|------|---|---|---|-------------|---------|---------|
| 高可靠性 | 4 | 4 | 8 | 5 | 4个磁盘 | 50% |
| 平衡 | 4 | 2 | 6 | 5 | 2个磁盘 | 67% |
| 高效率 | 8 | 2 | 10 | 9 | 2个磁盘 | 80% |
| 最小冗余 | 2 | 1 | 3 | 3 | 1个磁盘 | 67% |

**选择建议**：
- **生产环境**：建议使用4+2或4+4配置
- **开发/测试**：可使用2+1配置
- **大规模存储**：可考虑8+2配置

### 超时配置

```rust
QuorumConfig {
    data_shards: 4,
    parity_shards: 2,
    disk_health_timeout_secs: 3,  // 建议3-5秒
}
```

- **太短**（<2秒）：可能导致误判
- **太长**（>10秒）：故障检测延迟过高
- **推荐**：3-5秒，与连接超时配置一致

## 故障场景

### 场景1：单磁盘故障

**配置**：4+2，写入法定人数=5

```
初始状态：6个磁盘全部在线
   ↓
disk1故障
   ↓
5个磁盘在线（满足法定人数）
   ↓
写入继续，数据分布到5个磁盘
   ↓
系统正常运行
```

### 场景2：多磁盘故障

**配置**：4+2，写入法定人数=5

```
初始状态：6个磁盘全部在线
   ↓
disk1和disk2同时故障
   ↓
4个磁盘在线（<5，不满足法定人数）
   ↓
QuorumManager暂停写入
   ↓
客户端收到"法定人数不足"错误
   ↓
磁盘恢复后自动恢复写入
```

### 场景3：逐步恢复

```
T=0: 2个磁盘故障，4个在线
     → 写入暂停

T=30s: disk1恢复，5个在线
       → 电路断路器尝试恢复
       → 写入自动恢复

T=60s: disk2恢复，6个在线
       → 全部恢复
```

## 性能影响

### 开销分析

| 操作 | 延迟影响 | CPU影响 | 说明 |
|------|---------|---------|------|
| verify_write_quorum | <1ms | 可忽略 | 原子操作 |
| record_disk_result | <1ms | 可忽略 | 原子操作+锁 |
| eliminate_offline_disks | <10ms | 低 | 后台任务，10秒一次 |
| get_stats | <1ms | 可忽略 | 只读操作 |

### 内存占用

- 每个DiskHealth：约200字节
- 6个磁盘配置：约1.2KB
- 可忽略不计

## 测试

### 单元测试

运行测试：

```bash
cargo test --package rustfs-ecstore --lib quorum_manager
```

覆盖场景：
- ✅ 法定人数计算
- ✅ 磁盘状态转换
- ✅ 法定人数验证
- ✅ 写入暂停状态

### 集成测试建议

```rust
#[tokio::test]
async fn test_write_with_disk_failure() {
    // 1. 创建QuorumManager
    let manager = create_test_manager();
    
    // 2. 验证初始状态
    assert!(manager.verify_write_quorum().await.is_ok());
    
    // 3. 模拟磁盘故障
    for _ in 0..3 {
        manager.record_disk_result("disk1", false).await;
    }
    
    // 4. 验证仍满足法定人数（5个在线）
    assert!(manager.verify_write_quorum().await.is_ok());
    
    // 5. 模拟更多故障
    for _ in 0..3 {
        manager.record_disk_result("disk2", false).await;
    }
    
    // 6. 验证不满足法定人数（4个在线）
    assert!(manager.verify_write_quorum().await.is_err());
    
    // 7. 恢复磁盘
    manager.record_disk_result("disk1", true).await;
    
    // 8. 验证恢复后满足法定人数
    assert!(manager.verify_write_quorum().await.is_ok());
}
```

## 故障排除

### 问题：写入一直被阻止

**症状**：
```
ERROR Write quorum NOT met: only 4/6 disks available (required: 5)
```

**排查步骤**：

1. 检查磁盘状态：
   ```rust
   let stats = manager.get_stats().await;
   println!("在线: {}, 离线: {}", stats.online_disks, stats.offline_disks);
   ```

2. 检查电路断路器状态：
   ```rust
   use rustfs_common::globals::GLOBAL_CIRCUIT_BREAKERS;
   let stats = GLOBAL_CIRCUIT_BREAKERS.get_stats().await;
   ```

3. 检查磁盘连接性：
   - 网络连接
   - 磁盘挂载状态
   - 防火墙规则

4. 手动重置电路断路器（如果需要）：
   ```rust
   GLOBAL_CIRCUIT_BREAKERS.reset("disk_id").await;
   ```

### 问题：频繁的磁盘状态切换

**症状**：
```
WARN Disk disk1 marked offline after 3 failures
INFO Disk disk1 recovered to online
WARN Disk disk1 marked offline after 3 failures
...
```

**可能原因**：
- 网络不稳定
- 磁盘负载过高
- 健康检查超时设置过短

**解决方案**：
1. 增加超时时间
2. 检查网络质量
3. 优化磁盘性能

## 最佳实践

### 1. 合理配置EC参数

- 根据预期故障率选择P值
- 考虑网络带宽和延迟
- 平衡可靠性和存储效率

### 2. 监控关键指标

- 设置告警规则
- 定期检查离线磁盘
- 监控阻止的写入率

### 3. 快速响应故障

- 自动化故障检测
- 及时修复离线磁盘
- 保持足够的冗余

### 4. 定期测试

- 模拟磁盘故障
- 验证恢复流程
- 测试边界情况

## 参考

- [连接健康跟踪文档](cluster-power-failure-recovery.md)
- [电路断路器实现](../crates/common/src/circuit_breaker.rs)
- [QuorumManager源码](../crates/ecstore/src/quorum_manager.rs)
- [纠删码实现](../crates/ecstore/src/erasure_coding/erasure.rs)
