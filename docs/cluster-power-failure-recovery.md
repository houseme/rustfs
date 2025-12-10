# RustFS集群电源故障恢复解决方案

## 概述

本文档描述了RustFS集群在节点突然断电场景下的恢复机制。该解决方案实现了多层防御架构，确保集群在部分节点故障时仍能保持可用性。

## 问题描述

### 故障场景

当4节点RustFS集群中某个节点突然断电时，会出现以下问题：

1. **整个集群变得无响应** - 其他健康节点也无法正常服务
2. **文件上传失败** - 数据平面操作阻塞
3. **控制台Web UI卡死** - 管理平面无法响应
4. **管理操作超时** - 集群协调操作挂起

### 根本原因

- **TCP连接缓存未检测死连接** - 缓存的gRPC连接在节点断电后不会自动失效
- **无主动健康检查** - 系统仅在有错误时被动淘汰连接
- **阻塞等待** - 操作尝试使用死连接时会长时间挂起
- **缺乏快速失败机制** - 未能在3-5秒内检测并隔离故障节点

## 解决方案架构

### 第一层：主动连接管理（Active Connection Management）

#### 连接健康跟踪

实现了`ConnectionHealth`结构体来跟踪每个连接的健康状态：

```rust
pub struct ConnectionHealth {
    pub addr: String,
    pub last_successful_use: RwLock<SystemTime>,
    pub consecutive_failures: AtomicU32,
    pub last_health_check: RwLock<SystemTime>,
    pub health_state: AtomicU8,  // Healthy/Degraded/Dead
}
```

**健康状态转换规则：**

- **Healthy (0)** - 连接正常工作
- **Degraded (1)** - 1-2次连续失败
- **Dead (2)** - 3次或以上连续失败

#### 后台健康检查器

启动了一个后台任务，每10秒检查并驱逐不健康的连接：

```rust
pub fn start_connection_health_checker(interval_secs: u64) -> JoinHandle<()>
```

**驱逐标准：**

1. 连接状态为Dead
2. 30秒内未成功使用
3. 连续失败次数≥3
4. 健康检查时间戳过期（超过30秒）

#### 与电路断路器双重保护

连接健康跟踪与现有的电路断路器模式协同工作：

- **电路断路器** - 防止向故障节点重复发送请求（3次失败后打开，30秒后尝试恢复）
- **连接健康跟踪** - 主动驱逐缓存中的死连接

两层保护确保：
1. 快速检测故障（3-8秒）
2. 自动隔离故障节点
3. 优雅恢复（节点恢复后自动重新集成）

### 第二层：操作特定超时（Operation-Specific Timeouts）

已在`crates/protos/src/lib.rs`中配置的超时参数：

```rust
const CONNECT_TIMEOUT_SECS: u64 = 3;           // 连接建立超时
const TCP_KEEPALIVE_SECS: u64 = 10;            // TCP保活探测
const HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;  // HTTP/2 PING间隔
const HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;   // HTTP/2 PING超时
const RPC_TIMEOUT_SECS: u64 = 30;              // RPC操作总超时
```

这些配置确保：
- **快速连接超时** - 3秒内检测到节点不可达
- **积极的保活探测** - 5秒HTTP/2 PING + 10秒TCP keepalive
- **空闲连接检测** - 即使没有活跃请求也会发送PING

### 第三层：电路断路器集成

电路断路器已在以下关键组件中集成：

#### 1. 通知系统（notification_sys.rs）

```rust
if !rustfs_common::globals::should_attempt_peer(&host).await {
    warn!("peer {} circuit breaker open, skipping", host);
    return offline_response();
}

match timeout(peer_timeout, operation).await {
    Ok(Ok(result)) => {
        rustfs_common::globals::record_peer_success(&host).await;
        result
    }
    _ => {
        rustfs_common::globals::record_peer_failure(&host).await;
        offline_response()
    }
}
```

#### 2. Peer REST客户端（peer_rest_client.rs）

实现了自动连接驱逐：

```rust
pub async fn evict_connection(&self) {
    evict_failed_connection(&self.grid_host).await;
}
```

每个RPC调用失败时自动驱逐连接。

### 第四层：监控和指标

新增的Prometheus指标：

| 指标名称 | 类型 | 描述 |
|---------|------|------|
| `health_connections_cached_count` | Gauge | 缓存的gRPC连接总数 |
| `health_connections_healthy_count` | Gauge | 健康连接数 |
| `health_connections_degraded_count` | Gauge | 降级连接数 |
| `health_connections_dead_count` | Gauge | 死亡连接数 |
| `health_connections_evicted_total` | Counter | 累计驱逐的连接数 |
| `health_circuit_breakers_open_count` | Gauge | 打开的电路断路器数 |
| `health_circuit_breakers_half_open_count` | Gauge | 半开状态的电路断路器数 |

## 使用指南

### 监控连接健康

使用提供的API获取连接健康统计：

```rust
use rustfs_common::globals::get_connection_health_stats;

let stats = get_connection_health_stats().await;
for (addr, (state, failures, idle_secs)) in stats {
    println!("{}: {:?}, failures={}, idle={}s", addr, state, failures, idle_secs);
}
```

### 手动驱逐连接

在某些情况下，您可能需要手动驱逐连接：

```rust
use rustfs_common::globals::evict_connection;

evict_connection("http://node1:9000").await;
```

### 清空所有连接

在集群维护或恢复场景：

```rust
use rustfs_common::globals::clear_all_connections;

clear_all_connections().await;
```

## 测试

### 单元测试

运行连接健康跟踪测试：

```bash
cargo test --package rustfs-common --test connection_health_test
```

### 集成测试场景

1. **节点突然断电恢复测试**
   - 启动4节点集群
   - 硬关闭一个节点（kill -9或断电）
   - 验证其他节点在3-8秒内检测到故障
   - 验证文件上传继续工作
   - 验证控制台保持响应

2. **节点重新上线测试**
   - 重启之前关闭的节点
   - 验证电路断路器在30秒后尝试恢复
   - 验证连接自动重新建立
   - 验证集群完全恢复功能

## 配置参数

### 环境变量

虽然当前实现使用硬编码的合理默认值，但可以通过以下方式调整：

| 参数 | 默认值 | 描述 |
|------|--------|------|
| 健康检查间隔 | 10秒 | 后台任务检查频率 |
| 最大空闲时间 | 30秒 | 驱逐未使用连接的时间 |
| 最大失败次数 | 3次 | 标记为Dead的失败阈值 |
| 电路断路器阈值 | 3次 | 打开电路的失败次数 |
| 恢复超时 | 30秒 | 电路从Open到Half-Open的时间 |

## 性能影响

- **CPU开销** - 可忽略（后台任务每10秒运行一次）
- **内存开销** - 每个跟踪的连接约100字节
- **延迟影响** - 无（检查在后台进行）
- **网络开销** - HTTP/2 PING每5秒，TCP keepalive每10秒

## 已知限制

1. **Lock服务** - 分布式锁客户端尚未集成电路断路器（锁操作的关键性质需要特殊处理）
2. **冷启动** - 首次连接失败会经历完整的超时周期
3. **指标导出** - Prometheus指标定义已添加，但尚未实现导出器

## 故障排除

### 连接频繁被驱逐

检查：
- 网络稳定性
- 节点负载
- 防火墙配置

解决方案：
- 增加`MAX_IDLE_SECS`
- 检查节点健康状态
- 审查网络配置

### 电路断路器一直打开

检查：
- 目标节点是否真的宕机
- 网络连接性
- 防火墙规则

解决方案：
- 修复节点问题
- 手动重置电路断路器
- 检查日志了解失败原因

## 参考

- [电路断路器模式](crates/common/src/circuit_breaker.rs)
- [连接健康跟踪](crates/common/src/globals.rs)
- [gRPC连接管理](crates/protos/src/lib.rs)
- [通知系统集成](crates/ecstore/src/notification_sys.rs)
