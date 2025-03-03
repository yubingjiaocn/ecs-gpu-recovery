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

2. **DCGM Task Monitor** (`dcgm_task_monitor.py`)
   - Monitors completion of DCGM health check tasks
   - Analyzes DCGM results to determine GPU health
   - Initiates instance reboot when necessary
   - Updates job and container instance status in DynamoDB

3. **ECS Instance Monitor** (`ecs_instance_monitor.py`)
   - Monitors ECS container instance state changes
   - Detects instances returning to service after reboot
   - Re-executes training tasks on recovered instances
   - Sends notifications when recovery fails

### DynamoDB Tables

1. **hybrid-gpu-training-job**
   - Tracks GPU training job status
   - Stores metadata about jobs, tasks, and recovery attempts

2. **ecs_container_instance**
   - Tracks ECS container instance status
   - Records reboots and recovery operations

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
        CheckExit0 -->|No| NoAction1[No Action]
    end

    RunDCGM --> DCGMEvent[DCGM Task Completion Event]

    DCGMEvent --> DCGMMonitor[DCGM Task Monitor Lambda]

    subgraph "GPU Health Analysis"
        DCGMMonitor --> CheckDCGM{DCGM Exit Code}
        CheckDCGM -->|0| MarkFailed[Mark Job as Failed]
        CheckDCGM -->|1| CheckRunTime{Run Time = 1?}
        CheckRunTime -->|Yes| RebootInstance[Reboot Instance]
        CheckRunTime -->|No| MarkJobFailed[Mark Job as Failed]
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
</mermaid>

## Setup and Deployment

This project is built using the AWS Cloud Development Kit (CDK) with Python.

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.13 or later
- AWS CDK toolkit installed

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

- `TRAINING_JOB_TABLE_NAME`: DynamoDB table for tracking training jobs
- `CONTAINER_INSTANCE_TABLE_NAME`: DynamoDB table for tracking container instances
- `ECS_CLUSTER_NAME`: Name of the ECS cluster running GPU tasks
- `DCGM_HEALTH_CHECK_TASK`: Task definition name for the DCGM health check
- `SNS_TOPIC_ARN`: ARN of the SNS topic for notifications

## Other Useful Commands

- `cdk ls`: List all stacks in the app
- `cdk diff`: Compare deployed stack with current state
- `cdk docs`: Open CDK documentation
