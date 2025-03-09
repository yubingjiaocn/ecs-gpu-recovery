# ECS GPU Recovery System State Graphs Summary

This document provides a summary of the state transitions for the three main components of the ECS GPU Recovery System: Jobs, Tasks, and Nodes (Container Instances).

## Component Relationships

The ECS GPU Recovery system tracks three main components that work together to enable GPU task recovery:

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
    JS4[RESTARTING]
    JS5[COMPLETE]
    JS6[FAILED]
    JS7[USER_STOPPED]
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
    NS5[REBOOTING]
    NS6[FAILED]
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
   - When DCGM health check completes with GPU issue detected:
     - Job transitions from `PENDING_HEALTHCHECK` to `PENDING_RESTART`
     - Node transitions from `PENDING_HEALTHCHECK` to `REBOOTING`
   - When DCGM health check completes with no GPU issue:
     - Job transitions from `PENDING_HEALTHCHECK` to `FAILED`
     - Node transitions from `PENDING_HEALTHCHECK` to `AVAILABLE`

3. **Recovery Process**:
   - When node becomes available after reboot:
     - Node transitions from `REBOOTING` to `AVAILABLE`
     - Job transitions from `PENDING_RESTART` to `RESTARTING`
     - Job then transitions from `RESTARTING` to `IN_PROGRESS` when tasks restart
     - New tasks are created in `IN_PROGRESS` state

4. **Successful Completion**:
   - When all tasks transition to `COMPLETE`:
     - Job transitions to `COMPLETE`
     - Nodes remain in `AVAILABLE` state

5. **User Intervention**:
   - When user stops tasks:
     - Tasks transition to `TERMINATED`
     - Job transitions to `USER_STOPPED`
     - Nodes transition to `AVAILABLE`

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
    Node->>Node: REBOOTING

    Note over Job,Node: Instance Reboot
    Node->>Node: Reboot
    Node->>Node: AVAILABLE

    Note over Job,Node: Recovery
    Job->>Job: RESTARTING
    Job->>Task: Create new task
    Task->>Task: IN_PROGRESS
    Job->>Job: IN_PROGRESS
    Task->>Node: Assign to node
    Node->>Node: IN_PROGRESS
```

## Lambda Function Responsibilities

Each Lambda function in the system is responsible for specific state transitions:

1. **ECS Task Handler**:
   - Detects task failures (exit code 1)
   - Transitions job to `PENDING_HEALTHCHECK`
   - Transitions node to `PENDING`
   - Launches DCGM health check task

2. **DCGM Task Monitor**:
   - Analyzes health check results
   - Transitions job to `PENDING_RESTART` or `FAILED`
   - Transitions node to `REBOOTING` or `AVAILABLE`
   - Initiates instance reboot if needed

3. **ECS Instance Monitor**:
   - Detects when instances return to service
   - Transitions job from `PENDING_RESTART` to `RESTARTING` to `IN_PROGRESS`
   - Transitions node from `REBOOTING` to `AVAILABLE`
   - Creates new tasks to replace failed ones

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
