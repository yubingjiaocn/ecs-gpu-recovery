# DynamoDB Schema Documentation

This document describes the schema for the DynamoDB tables used by the ECS GPU Recovery system.

## Task Table (`ecs_task`)

The task table stores information about individual ECS tasks that run on container instances.

### Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| ecs_task_id | String (PK) | The ECS task ID |
| node_name | String | The name of the node this task is running on |
| node_index_in_job | String | The index of this node in the job |
| job_id | String | The job ID that this task belongs to |
| job_timestamp | String | The timestamp when the job was created |
| job_num_nodes | String | The number of nodes in the job |
| task_def_arn | String | The ARN of the task definition |
| task_def_name | String | The name of the task definition |
| task_def_revision | String | The revision of the task definition |
| cluster_name | String | The name of the ECS cluster |
| container_inst_id | String | The container instance ID |
| container_instance_arn | String | The ARN of the container instance |
| retry | String | The number of retry attempts (starts at 0) |
| task_status | String | The current status of the task (IN_PROGRESS, COMPLETE, TERMINATED) |
| updated_at | String | The timestamp when the task was last updated |
| created_at | String | The timestamp when the task was created |

### Task Status Values

- `IN_PROGRESS`: Task is currently running
- `COMPLETE`: Task completed successfully (exit code 0)
- `TERMINATED`: Task was terminated due to failure (exit code 1) or user intervention

## Job Table (`ecs_job`)

The job table stores information about jobs, which are collections of related tasks.

### Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| job_id | String (PK) | The job ID |
| job_timestamp | String | The timestamp when the job was created |
| cluster_name | String | The name of the ECS cluster |
| num_nodes | String | The number of nodes in the job |
| assigned_nodes | List | The list of nodes assigned to this job |
| submitted_container_inst_ids | List | The list of container instance IDs |
| submitted_ecs_task_ids | List | The list of ECS task IDs |
| updated_at | String | The timestamp when the job was last updated |
| created_at | String | The timestamp when the job was created |
| retry | String | The number of retry attempts (starts at 0) |
| job_status | String | The current status of the job |

### Job Status Values

- `IN_PROGRESS`: Job is currently running with active tasks
- `PENDING_HEALTHCHECK`: Task failure detected, waiting for DCGM health check results
- `PENDING_RESTART`: GPU issue confirmed, waiting for instance to recover
- `RESTARTING`: Job is in the process of being restarted
- `COMPLETE`: All tasks completed successfully
- `FAILED`: Job failed after exhausting recovery attempts
- `USER_STOPPED`: Job was manually stopped by user

## Node Table (`ecs_node`)

The node table stores information about container instances (nodes) that host tasks.

### Schema

| Attribute | Type | Description |
|-----------|------|-------------|
| node_name | String (PK) | The name of the node |
| container_instance_id | String | The container instance ID |
| container_instance_arn | String | The ARN of the container instance |
| cluster_name | String | The name of the ECS cluster |
| node_status | String | The current status of the node |
| ip | String | The IP address of the node |
| ibdev | String | The InfiniBand device information |
| created_at | String | The timestamp when the node record was created |
| updated_at | String | The timestamp when the node was last updated |

### Node Status Values

- `AVAILABLE`: Node is healthy and available to run tasks
- `UNKNOWN`: Node status is unknown
- `ASSIGNED`: Node has been assigned to a job
- `IN_PROGRESS`: Node is currently running tasks
- `PENDING`: Task failure detected, node is waiting for health check
- `PENDING_HEALTHCHECK`: DCGM health check is running on the node
- `REBOOTING`: Node is being rebooted
- `FAILED`: Node has failed health check and is marked as unhealthy
