# ECS GPU Recovery System

This project implements an automated recovery system for GPU-accelerated tasks running on Amazon ECS (Elastic Container Service). It monitors task and instance health, detects GPU failures, and provides self-healing capabilities through a series of AWS serverless components.

## Architecture

The ECS GPU Recovery system consists of the following components:

- **AWS Lambda Functions**: Three Lambda functions that handle different aspects of monitoring and recovery
- **Amazon DynamoDB Tables**: Store state information about jobs and container instances
- **Amazon EventBridge Rules**: Trigger Lambda functions in response to ECS events
- **Amazon SNS Topic**: Send notifications about critical events

## System Components

### Lambda Functions

1. **ECS Task Handler** (`ecs_task_handler.py`)
   - Monitors ECS task state changes
   - Detects task failures (exit code 1)
   - Stops other tasks belonging to the same job
   - Triggers DCGM (NVIDIA Data Center GPU Manager) health check tasks
   - Updates job status to 'STOPPED' when failures are detected

2. **DCGM Task Monitor** (`dcgm_task_monitor.py`)
   - Monitors completion of DCGM health check tasks
   - Analyzes DCGM results to determine GPU health
   - Initiates instance reboot when necessary
   - Updates job and container instance status in DynamoDB
   - Handles different exit codes from DCGM tasks

3. **ECS Instance Monitor** (`ecs_instance_monitor.py`)
   - Monitors ECS container instance state changes
   - Detects instances returning to service after reboot
   - Re-executes training tasks on recovered instances
   - Sends notifications via SNS when recovery fails
   - Tracks run attempts to prevent infinite recovery loops

### DynamoDB Tables

1. **ecs-job-sub**
   - Tracks GPU training job status
   - Stores metadata about jobs, tasks, and recovery attempts
   - Uses job_id_rank as the primary key

2. **ecs_container_instance**
   - Tracks ECS container instance status
   - Records reboots and recovery operations
   - Uses container_inst_id as the primary key

### Event Flow

```mermaid
flowchart TD
    TaskEvent[ECS Task State Change Event] --> TaskHandler[ECS Task Handler Lambda]

    subgraph "Task Failure Detection"
        TaskHandler --> CheckExit{Exit Code = 1?}
        CheckExit -->|Yes| StopTasks[Stop Related Tasks]
        StopTasks --> RunDCGM[Run DCGM Health Check]
        CheckExit -->|No| CheckExit0{Exit Code = 0?}
        CheckExit0 -->|Yes| UpdateComplete[Update Job Status to Complete]
        CheckExit0 -->|No| CheckUserStopped{User Stopped?}
        CheckUserStopped -->|Yes| MarkOther[Mark Job as Other]
        CheckUserStopped -->|No| NoAction1[No Action]
    end

    RunDCGM --> DCGMEvent[DCGM Task Completion Event]

    DCGMEvent --> DCGMMonitor[DCGM Task Monitor Lambda]

    subgraph "GPU Health Analysis"
        DCGMMonitor --> CheckDCGM{DCGM Exit Code}
        CheckDCGM -->|0| MarkFailed[Mark Job as FAILED]
        CheckDCGM -->|1| CheckRunTime{Run Time = 1?}
        CheckRunTime -->|Yes| RebootInstance[Reboot Instance]
        CheckRunTime -->|No| MarkJobFailed[Mark Job as FAILED]
    end

    RebootInstance --> InstanceEvent[Instance State Change Event]

    InstanceEvent --> InstanceMonitor[ECS Instance Monitor Lambda]

    subgraph "Instance Recovery"
        InstanceMonitor --> CheckActive{Instance Active?}
        CheckActive -->|Yes| FindJobs[Find Associated Jobs]
        CheckActive -->|No| NoAction2[No Action]
        FindJobs --> CheckJobRunTime{Job Run Time = 1?}
        CheckJobRunTime -->|Yes| RestartTask[Restart Training Task]
        CheckJobRunTime -->|No| SendNotif[Send SNS Notification]
    end
```

## Setup and Deployment

This project is built using the AWS Cloud Development Kit (CDK) with Python.

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.13 or later
- AWS CDK toolkit installed (v2.180.0 or later)

### Setup Virtual Environment

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # Unix/macOS
.venv\Scripts\activate.bat  # Windows

# Install requirements
pip install -r requirements.txt
```

### Deploy the Stack

```bash
# Synthesize the CloudFormation template
cdk synth

# Deploy the stack
cdk deploy
```

## Key Environment Variables

- `TRAINING_JOB_TABLE_NAME`: DynamoDB table for tracking training jobs (ecs-job-sub)
- `CONTAINER_INSTANCE_TABLE_NAME`: DynamoDB table for tracking container instances (ecs_container_instance)
- `ECS_CLUSTER_NAME`: Name of the ECS cluster running GPU tasks (nwcd-gpu-testing)
- `DCGM_HEALTH_CHECK_TASK`: Task definition ARN for the DCGM health check
- `SNS_TOPIC_ARN`: ARN of the SNS topic for notifications (gpu-training-notifications)

## Other Useful Commands

- `cdk ls`: List all stacks in the app
- `cdk diff`: Compare deployed stack with current state
- `cdk docs`: Open CDK documentation

## Testing

The project includes a test suite for testing the ECS GPU Recovery workflow with mock containers. See [tests/integration/README.md](tests/integration/README.md) for details.

### Test Components

1. **Task Definitions**: Three mock task definitions using busybox images:
   - `mock-failed-training-task`: Exits with code 1 after 120 seconds
   - `mock-successful-training-task`: Runs indefinitely
   - `mock-dcgm-health-check`: Exits with code 1 after 60 seconds

2. **Test Script**: A Python script that:
   - Creates the task definitions if they don't exist
   - Launches a test job with two tasks (one that fails and one that succeeds)
   - Creates the necessary DynamoDB records
   - Monitors the task status

### Running Tests

```bash
# Run the test suite
python tests/integration/run_tests.py

# Run with custom parameters
python tests/integration/run_tests.py \
  --cluster your-cluster-name \
  --task-table your-task-table \
  --job-table your-job-table \
  --node-table your-node-table

# Only set up task definitions without running tests
python tests/integration/run_tests.py --setup-only
```
