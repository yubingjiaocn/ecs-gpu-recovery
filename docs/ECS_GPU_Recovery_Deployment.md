# ECS GPU Recovery System Deployment Guide

This document provides detailed deployment steps for the ECS GPU Recovery system, from installing the AWS CDK CLI to completing the deployment process.

## Table of Contents

- [System Overview](#system-overview)
- [Prerequisites](#prerequisites)
- [Installing AWS CDK CLI](#installing-aws-cdk-cli)
- [Setting Up the Project](#setting-up-the-project)
- [Configuring Deployment](#configuring-deployment)
- [Deploying the Solution](#deploying-the-solution)
- [Verifying Deployment](#verifying-deployment)
- [Required Permissions](#required-permissions)

## System Overview

The ECS GPU Recovery system is an automated solution for monitoring and recovering GPU resources in Amazon ECS clusters. When the system detects a GPU-accelerated task failure, it runs diagnostics to determine if the failure is GPU-related and takes appropriate recovery actions, including instance reboots and task restarts.

### Key Features

- Automatic detection of GPU task failures
- NVIDIA DCGM health checks for GPU diagnostics
- Self-healing through instance reboots
- Automatic task restart after recovery
- Notification system for unrecoverable failures
- Retry limiting to prevent infinite recovery loops

### Architecture Components

- **Lambda Functions**
  - **ECS Task Handler**: Monitors task failures and triggers health checks
  - **DCGM Task Monitor**: Analyzes GPU health check results
  - **ECS Instance Monitor**: Handles instance recovery and task restarts

- **DynamoDB Tables**
  - **ecs_task**: Tracks individual task status
  - **ecs_job**: Manages job status (collection of related tasks)
  - **ecs_node**: Monitors container instance health

- **Event Triggers**
  - EventBridge rules for task state changes
  - EventBridge rules for container instance state changes

## Prerequisites

Before starting deployment, ensure you meet the following requirements:

1. **AWS Account**: An active AWS account with permissions to create and manage resources
2. **AWS CLI**: Installed and configured AWS CLI with appropriate permissions
3. **Python**: Python 3.11 or higher installed
4. **Node.js**: Node.js 14.x or higher installed (CDK dependency)
5. **Development Environment**: A development environment with terminal access

## Installing AWS CDK CLI

AWS CDK (Cloud Development Kit) is a development framework that allows you to define cloud infrastructure using familiar programming languages. Follow these steps to install the CDK CLI:

### Step 1: Install Node.js (if not already installed)

```bash
# For Ubuntu/Debian
sudo apt update
sudo apt install -y nodejs npm

# For Amazon Linux/RHEL/CentOS
sudo yum install -y nodejs npm

# For macOS (using Homebrew)
brew install node

# For Windows
# Download and install Node.js from https://nodejs.org
```

### Step 2: Install AWS CDK CLI

```bash
# Install CDK CLI globally
npm install -g aws-cdk
```

### Step 3: Verify Installation

```bash
# Check CDK version
cdk --version
```

Ensure CDK version is 2.180.0 or higher.

## Setting Up the Project

### Step 1: Clone the Repository

```bash
# Clone the repository
git clone https://github.com/yourusername/ecs-gpu-recovery.git
cd ecs-gpu-recovery
```

### Step 2: Create and Activate Virtual Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate on Linux/macOS
source .venv/bin/activate

# Activate on Windows
.venv\Scripts\activate.bat
```

### Step 3: Install Dependencies

```bash
# Install project dependencies
pip install -r requirements.txt
```

### Step 4: Bootstrap AWS Environment (First-time CDK use)

If this is your first time using CDK in your AWS account/region, you need to bootstrap the environment:

```bash
# Bootstrap with default profile
cdk bootstrap

# Or specify account and region
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

## Configuring Deployment

Before deploying the solution, you may need to adjust the configuration for your environment.

### Method 1: Using Environment Variables

You can override default configuration by setting environment variables:

```bash
# Set DynamoDB table names
export TASK_TABLE_NAME="my_ecs_task"
export JOB_TABLE_NAME="my_ecs_job"
export NODE_TABLE_NAME="my_ecs_node"

# Set ECS cluster name
export ECS_CLUSTER_NAME="my-gpu-cluster"

# Set DCGM health check task definition
export DCGM_HEALTH_CHECK_TASK="arn:aws:ecs:region:account:task-definition/gpu-dcgm-health-check:1"

# Set Lambda configuration
export LAMBDA_TIMEOUT_SECONDS=120
export LAMBDA_MEMORY_SIZE=512

# Set SNS configuration
export SNS_TOPIC_NAME="my-gpu-notifications"
export SNS_TOPIC_DISPLAY_NAME="My GPU Training Job Notifications"
```

### Method 2: Modifying Configuration File

Alternatively, you can directly modify default values in `ecs_gpu_recovery/config.py`:

```python
# Edit configuration file
class Config:
    # DynamoDB Tables
    TASK_TABLE_NAME = "my_ecs_task"
    JOB_TABLE_NAME = "my_ecs_job"
    NODE_TABLE_NAME = "my_ecs_node"

    # ECS Configuration
    ECS_CLUSTER_NAME = "my-gpu-cluster"
    DCGM_HEALTH_CHECK_TASK = "arn:aws:ecs:region:account:task-definition/gpu-dcgm-health-check:1"

    # Other configuration...
```

## Deploying the Solution

### Step 1: Synthesize CloudFormation Template

```bash
# Synthesize CloudFormation template
cdk synth
```

This generates CloudFormation templates in the `cdk.out` directory.

### Step 2: Deploy the Stack

```bash
# Deploy the stack
cdk deploy
```

During deployment, CDK will display the list of resources to be created and ask for confirmation. Enter `y` to confirm deployment.

To skip the confirmation step, use the `--require-approval never` option:

```bash
cdk deploy --require-approval never
```

### Step 3: Review Deployment Output

After deployment completes, CDK will display output information, including ARNs of created resources and other important information.

## Verifying Deployment

After deployment, verify success with these steps:

### Step 1: Check CloudFormation Stack

```bash
# List CloudFormation stacks
aws cloudformation describe-stacks --stack-name EcsGpuRecoveryStack
```

Ensure stack status is `CREATE_COMPLETE`.

### Step 2: Check DynamoDB Tables

```bash
# List DynamoDB tables
aws dynamodb list-tables
```

Confirm `ecs_task`, `ecs_job`, and `ecs_node` tables (or your custom table names) are created.

### Step 3: Check Lambda Functions

```bash
# List Lambda functions
aws lambda list-functions
```

Confirm `EcsTaskHandler`, `DcgmTaskMonitor`, and `EcsInstanceMonitor` functions are created.

### Step 4: Check EventBridge Rules

```bash
# List EventBridge rules
aws events list-rules
```

Confirm rules for monitoring ECS task and instance state changes are created.

### Step 5: Check SNS Topic

```bash
# List SNS topics
aws sns list-topics
```

Confirm notification topic is created.

## Required Permissions

To successfully deploy this solution, you need specific AWS permissions. See [Permissions Documentation](./ECS_GPU_Recovery_Permissions.md) for a detailed list of required permissions.

## Troubleshooting

If you encounter issues during deployment, try these steps:

1. **Check CDK Version**: Ensure you're using CDK 2.180.0 or higher
2. **Check AWS Credentials**: Ensure your AWS credentials are valid and have sufficient permissions
3. **Check Logs**: Review CloudFormation and Lambda logs for error details
4. **Clean Up Resources**: If deployment fails, use `cdk destroy` to clean up created resources, then retry

If problems persist, refer to AWS CDK documentation or contact AWS Support.
