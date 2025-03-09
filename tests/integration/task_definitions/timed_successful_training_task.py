import boto3
import json
import time
from datetime import datetime

def create_timed_successful_training_task_definition():
    """
    Create a task definition for a mock successful training task that exits after 120 seconds.
    This task will exit with code 0 after 120 seconds.

    Returns:
        str: The ARN of the created task definition
    """
    ecs_client = boto3.client('ecs')

    # Check if task definition already exists
    try:
        response = ecs_client.describe_task_definition(
            taskDefinition='mock-timed-successful-training-task'
        )
        print(f"Task definition already exists: {response['taskDefinition']['taskDefinitionArn']}")
        return response['taskDefinition']['taskDefinitionArn']
    except ecs_client.exceptions.ClientException:
        # Task definition doesn't exist, create it
        pass

    # Create the task definition
    response = ecs_client.register_task_definition(
        family='mock-timed-successful-training-task',
        networkMode='bridge',
        requiresCompatibilities=['EC2'],
        cpu='256',
        memory='512',
        containerDefinitions=[
            {
                'name': 'timed-successful-training-container',
                'image': 'busybox:latest',
                'essential': True,
                'command': ['sh', '-c', 'echo "Starting mock successful training task"; for i in $(seq 1 12); do echo "Training in progress... $i/12"; sleep 10; done; echo "Training completed successfully"; exit 0']
            }
        ],
        tags=[
            {
                'key': 'Purpose',
                'value': 'Testing'
            },
            {
                'key': 'CreatedBy',
                'value': 'EcsGpuRecoveryTestSuite'
            },
            {
                'key': 'CreatedAt',
                'value': datetime.now().isoformat()
            }
        ]
    )

    task_definition_arn = response['taskDefinition']['taskDefinitionArn']
    print(f"Created task definition: {task_definition_arn}")
    return task_definition_arn

if __name__ == "__main__":
    create_timed_successful_training_task_definition()
