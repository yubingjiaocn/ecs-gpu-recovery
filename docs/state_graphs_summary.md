# ECS GPU Recovery System State Graphs Summary

This document provides a summary of the state transitions for the three main components of the ECS GPU Recovery System: Jobs, Tasks, and Nodes (Container Instances).

## Component Relationships

```mermaid
flowchart TD
    Job[Job] --> Task1[Task 1]
    Job --> Task2[Task 2]
    Task1 --> Node1[Node 1]
    Task2 --> Node2[Node 2]

    subgraph "Job States"
    JS1[IN_PROGRESS]
    JS2[PENDING_HEALTHCHECK]
    JS3[PENDING_RESTART]
    JS4[COMPLETE]
    JS5[FAILED]
    JS6[USER_STOPPED]
    end

    subgraph "Task States"
    TS1[IN_PROGRESS]
    TS2[COMPLETE]
    TS3[TERMINATED]
    end

    subgraph "Node States"
    NS1[AVAILABLE]
    NS2[IN_PROGRESS]
    NS3[PENDING]
    NS4[PENDING_HEALTHCHECK]
    NS5[FAILED]
    end
```

## State Transition Interactions

The three components (Jobs, Tasks, and Nodes) interact with each other during the recovery process:

1. **Task Failure Triggers Job and Node State Changes**:
   - When a task transitions from `IN_PROGRESS` to `TERMINATED` (exit code 1)
   - The associated job transitions from `IN_PROGRESS` to `PENDING_HEALTHCHECK`
   - The associated node transitions from `IN_PROGRESS` to `PENDING`

2. **DCGM Health Check Process**:
   - When DCGM health check starts:
     - Node transitions from `PENDING` to `PENDING_HEALTHCHECK`
   - When DCGM health check completes:
     - Job transitions from `PENDING_HEALTHCHECK` to `PENDING_RESTART` (if GPU issue detected)
     - Node transitions from `PENDING_HEALTHCHECK` to `FAILED` (if GPU issue detected)

3. **Recovery Process**:
   - When node becomes available after reboot:
     - Node transitions from `FAILED` to `AVAILABLE`
     - Job transitions from `PENDING_RESTART` to `IN_PROGRESS`
     - New tasks are created in `IN_PROGRESS` state

4. **Successful Completion**:
   - When all tasks transition to `COMPLETE`:
     - Job transitions to `COMPLETE`
     - Nodes remain in `AVAILABLE` state

## Recovery Workflow Sequence

```mermaid
sequenceDiagram
    participant Job
    participant Task
    participant Node
    participant DCGM

    Note over Job,Node: Normal Operation
    Job->>Job: IN_PROGRESS
    Task->>Task: IN_PROGRESS
    Node->>Node: IN_PROGRESS

    Note over Job,Node: Task Failure
    Task->>Task: TERMINATED (exit code 1)
    Task->>Job: Update status
    Job->>Job: PENDING_HEALTHCHECK
    Task->>Node: Update status
    Node->>Node: PENDING

    Note over Job,Node: Health Check
    Job->>DCGM: Launch health check
    Node->>Node: PENDING_HEALTHCHECK
    DCGM->>DCGM: Check GPU health
    DCGM->>Job: Report results
    DCGM->>Node: Report results

    Note over Job,Node: GPU Issue Detected
    Job->>Job: PENDING_RESTART
    Node->>Node: FAILED

    Note over Job,Node: Instance Reboot
    Node->>Node: Reboot
    Node->>Node: AVAILABLE

    Note over Job,Node: Recovery
    Job->>Job: IN_PROGRESS
    Job->>Task: Create new task
    Task->>Task: IN_PROGRESS
    Task->>Node: Assign to node
    Node->>Node: IN_PROGRESS
```

## Key Points

1. **Job State** represents the overall status of a batch of related tasks
2. **Task State** represents the status of individual container executions
3. **Node State** represents the health and availability of container instances

The recovery system monitors these states and transitions between them to:
- Detect GPU failures through task exit codes
- Verify GPU health using DCGM health checks
- Recover from failures by rebooting instances and restarting tasks
- Track retry attempts to prevent infinite recovery loops
- Notify administrators when recovery fails

For detailed state transitions of each component, refer to the individual state graph files:
- [Job State Graph](job_state_graph.md)
- [Task State Graph](task_state_graph.md)
- [Node State Graph](node_state_graph.md)
