# DynamoDB 架构文档

本文档描述了 ECS GPU 恢复系统使用的 DynamoDB 表的架构。

## 任务表 (`ecs_task`)

任务表存储有关在容器实例上运行的单个 ECS 任务的信息。

### 架构

| 属性 | 类型 | 描述 |
|-----------|------|-------------|
| ecs_task_id | 字符串 (主键) | ECS 任务 ID |
| node_name | 字符串 | 此任务运行的节点名称 |
| node_index_in_job | 字符串 | 此节点在作业中的索引 |
| job_id | 字符串 | 此任务所属的作业 ID |
| job_timestamp | 字符串 | 作业创建的时间戳 |
| job_num_nodes | 字符串 | 作业中的节点数量 |
| task_def_arn | 字符串 | 任务定义的 ARN |
| task_def_name | 字符串 | 任务定义的名称 |
| task_def_revision | 字符串 | 任务定义的修订版本 |
| cluster_name | 字符串 | ECS 集群的名称 |
| container_inst_id | 字符串 | 容器实例 ID |
| container_instance_arn | 字符串 | 容器实例的 ARN |
| retry | 字符串 | 重试尝试次数（从 0 开始） |
| task_status | 字符串 | 任务的当前状态 |
| updated_at | 字符串 | 任务最后更新的时间戳 |
| created_at | 字符串 | 任务创建的时间戳 |

### 任务状态值

- `IN_PROGRESS`: 任务当前正在运行
- `COMPLETE`: 任务成功完成（退出代码 0）
- `TERMINATED`: 任务因失败（退出代码 1）或用户干预而终止

## 作业表 (`ecs_job`)

作业表存储有关作业的信息，作业是相关任务的集合。

### 架构

| 属性 | 类型 | 描述 |
|-----------|------|-------------|
| job_id | 字符串 (主键) | 作业 ID |
| job_timestamp | 字符串 | 作业创建的时间戳 |
| cluster_name | 字符串 | ECS 集群的名称 |
| num_nodes | 字符串 | 作业中的节点数量 |
| assigned_nodes | 列表 | 分配给此作业的节点列表 |
| submitted_container_inst_ids | 列表 | 容器实例 ID 列表 |
| submitted_ecs_task_ids | 列表 | ECS 任务 ID 列表 |
| updated_at | 字符串 | 作业最后更新的时间戳 |
| created_at | 字符串 | 作业创建的时间戳 |
| retry | 字符串 | 重试尝试次数（从 0 开始） |
| job_status | 字符串 | 作业的当前状态 |

### 作业状态值

- `IN_PROGRESS`: 作业当前正在运行，有活动任务
- `PENDING_HEALTHCHECK`: 检测到任务失败，等待 DCGM 健康检查结果
- `PENDING_RESTART`: 确认 GPU 问题，等待实例恢复
- `RESTARTING`: 作业正在重新启动过程中
- `COMPLETE`: 所有任务成功完成
- `FAILED`: 作业在用尽恢复尝试后失败
- `USER_STOPPED`: 作业被用户手动停止

## 节点表 (`ecs_node`)

节点表存储有关托管任务的容器实例（节点）的信息。

### 架构

| 属性 | 类型 | 描述 |
|-----------|------|-------------|
| node_name | 字符串 (主键) | 节点的名称 |
| container_instance_id | 字符串 | 容器实例 ID |
| container_instance_arn | 字符串 | 容器实例的 ARN |
| cluster_name | 字符串 | ECS 集群的名称 |
| node_status | 字符串 | 节点的当前状态 |
| ip | 字符串 | 节点的 IP 地址 |
| ibdev | 字符串 | InfiniBand 设备信息 |
| created_at | 字符串 | 节点记录创建的时间戳 |
| updated_at | 字符串 | 节点最后更新的时间戳 |

### 节点状态值

- `AVAILABLE`: 节点健康且可用于运行任务
- `UNKNOWN`: 节点状态未知
- `ASSIGNED`: 节点已分配给作业
- `IN_PROGRESS`: 节点当前正在运行任务
- `PENDING`: 检测到任务失败，节点正在等待健康检查
- `PENDING_HEALTHCHECK`: DCGM 健康检查正在节点上运行
- `REBOOTING`: 节点正在重启
- `FAILED`: 节点未通过健康检查并被标记为不健康
