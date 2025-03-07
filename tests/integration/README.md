# ECS GPU Recovery Test Suite

This test suite provides tools for testing the ECS GPU Recovery system with mock containers.

## Overview

The test suite includes:

1. **Task Definitions**: Three mock task definitions using busybox images:
   - `mock-failed-training-task`: Exits with code 1 after 120 seconds
   - `mock-successful-training-task`: Runs indefinitely
   - `mock-dcgm-health-check`: Exits with code 1 after 60 seconds

2. **Test Script**: A Python script that:
   - Creates the task definitions if they don't exist
   - Launches a test job with two tasks (one that fails and one that succeeds)
   - Creates the necessary DynamoDB records
   - Monitors the task status

## Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.x with boto3 installed
- Access to an ECS cluster with at least 2 container instances
- DynamoDB tables for tasks, jobs, and nodes

## Usage

1. **Setup AWS Credentials**:
   ```bash
   export AWS_PROFILE=your-profile
   # Or
   export AWS_ACCESS_KEY_ID=your-access-key
   export AWS_SECRET_ACCESS_KEY=your-secret-key
   export AWS_REGION=your-region
   ```

2. **Run the Test Script**:
   ```bash
   cd /path/to/ecs-gpu-recovery
   python tests/integration/test_gpu_recovery_workflow.py \
     --cluster your-cluster-name \
     --task-table your-task-table \
     --job-table your-job-table \
     --node-table your-node-table
   ```

   If you're using the default values from the CDK stack, you can simply run:
   ```bash
   python tests/integration/test_gpu_recovery_workflow.py
   ```

3. **Monitor the Results**:
   - Check the CloudWatch logs for the Lambda functions
   - Check the DynamoDB tables for task and job status updates
   - The test script will monitor task status for 5 minutes (10 checks at 30-second intervals)

## Task Definitions

You can create the task definitions individually if needed:

```bash
python -c "from tests.integration.task_definitions import create_failed_training_task_definition; create_failed_training_task_definition()"
python -c "from tests.integration.task_definitions import create_successful_training_task_definition; create_successful_training_task_definition()"
python -c "from tests.integration.task_definitions import create_mock_dcgm_task_definition; create_mock_dcgm_task_definition()"
```

## Expected Workflow

1. The test script launches two tasks:
   - A failed training task (exits with code 1 after 120 seconds)
   - A successful training task (runs indefinitely)

2. When the failed task exits with code 1:
   - The ECS Task Handler Lambda should detect the failure
   - It should stop the successful task
   - It should run a DCGM health check task

3. When the DCGM task completes:
   - The DCGM Task Monitor Lambda should detect the completion
   - It should take appropriate action based on the exit code

4. If the instance is rebooted:
   - The ECS Instance Monitor Lambda should detect the instance coming back online
   - It should attempt to restart the tasks

## Troubleshooting

- **No container instances found**: Make sure your ECS cluster has active container instances
- **Task launch failures**: Check IAM permissions and ECS service limits
- **DynamoDB errors**: Verify table names and permissions
- **Lambda not triggered**: Check EventBridge rules and Lambda permissions
