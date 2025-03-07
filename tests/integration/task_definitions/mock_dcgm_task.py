import boto3
import json
import time
from datetime import datetime

def create_mock_dcgm_task_definition():
    """
    Create a task definition for a mock DCGM health check task.
    This task will exit with code 1 after 60 seconds.

    Returns:
        str: The ARN of the created task definition
    """
    ecs_client = boto3.client('ecs')

    # Check if task definition already exists
    try:
        response = ecs_client.describe_task_definition(
            taskDefinition='mock-dcgm-health-check'
        )
        print(f"Task definition already exists: {response['taskDefinition']['taskDefinitionArn']}")
        return response['taskDefinition']['taskDefinitionArn']
    except ecs_client.exceptions.ClientException:
        # Task definition doesn't exist, create it
        pass

    # Create the task definition
    response = ecs_client.register_task_definition(
        family='gpu-dcgm-health-check',
        networkMode='bridge',
        requiresCompatibilities=['EC2'],
        cpu='256',
        memory='512',
        containerDefinitions=[
            {
                'name': 'dcgm-health-check-container',
                'image': 'busybox:latest',
                'essential': True,
                'command': ['sh', '-c', 'echo "Starting mock DCGM health check"; sleep 60; echo "DCGM health check failed"; exit 1']
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
    create_mock_dcgm_task_definition()
