# Database Schema Documentation

## Task Submission Table

Records created after a single instance/task is successfully submitted (when task_id is returned).

**Primary Key**: `ecs_task_id`

```json
{
  "ecs_task_id": "task_id",
  "node_name": "node_name",
  "node_index_in_job": "nodei",
  "job_id": "job_id",
  "job_timestamp": "job_timestamp",
  "job_num_nodes": "nnodes",
  "task_def_arn": "task_def_arn",
  "task_def_name": "task_def_arn.split(':')[0]",
  "task_def_revision": "task_def_arn.split(':')[-1]",
  "cluster_name": "cluster_name",
  "container_inst_id": "container_inst_id",
  "retry": 0,
  "task_status": "IN_PROGRESS|USER_STOPPED|FAILED|PENDING_HEALTHCHECK|PENDING",
  "updated_at": "datetime.now().isoformat()",
  "created_at": "datetime.now().isoformat()"
}
```

## Job Status Table

Records created after an entire Job (containing N tasks) is submitted.

```json
{
  "job_id": "job_id",
  "job_timestamp": "job_timestamp",
  "cluster_name": "cluster_name",
  "num_nodes": "num_nodes",
  "assigned_nodes": "assigned_nodes",
  "submitted_container_inst_ids": "container_inst_ids",
  "submitted_ecs_task_ids": "ecs_task_ids",
  "updated_at": "datetime.now().isoformat()",
  "created_at": "datetime.now().isoformat()",
  "retry": 0,
  "job_status": "IN_PROGRESS|USER_STOPPED|FAILED|PENDING_HEALTHCHECK|PENDING"
}
```

## Node Information Table

```json
{
  "node_name": "node_name",
  "container_instance_id": "inst_arn.split('/')[-1]",
  "container_instance_arn": "inst_arn",
  "cluster_name": "cluster_name",
  "node_status": "AVAILABLE|UNKNOWN|ASSIGNED|IN_PROGRESS|PENDING_HEALTHCHECK|PENDING|REBOOTING|FAILED",
  "ip": "self.nodes.get(node_name).ip",
  "ibdev": "self.nodes.get(node_name).ibdev",
  "created_at": "datetime.datetime.now().isoformat()",
  "updated_at": "datetime.datetime.now().isoformat()"
}
```
