import boto3
import logging
from utils import error_handler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ECSService:
    """Service for ECS operations"""

    def __init__(self, cluster_name):
        """
        Initialize ECS service.

        Args:
            cluster_name (str): ECS cluster name
        """
        logger.info(f"[ECS_INIT] Initializing ECS service for cluster: {cluster_name}")
        self.cluster_name = cluster_name
        self.client = boto3.client('ecs')
        logger.info("[ECS_INIT_COMPLETE] ECS service initialized")

    @error_handler
    def set_instance_status(self, container_instance_arn, status):
        """
        Set the status attribute of a container instance.

        Args:
            container_instance_arn (str): Container instance ARN
            status (str): Status value to set

        Returns:
            dict: Response from put_attributes API
        """
        logger.info(f"[INSTANCE_ATTRIBUTE_CHANGE] Setting instance {container_instance_arn} status to {status}")
        response = self.client.put_attributes(
            cluster=self.cluster_name,
            attributes=[
                {
                    'name': 'status',
                    'value': status,
                    'targetType': 'container-instance',
                    'targetId': container_instance_arn
                }
            ]
        )
        logger.info(f"[INSTANCE_ATTRIBUTE_CHANGE_COMPLETE] Set instance {container_instance_arn} status to {status}")
        return response

    @error_handler
    def run_task(self, task_info):
        """
        Re-execute a task on a container instance.

        Args:
            task_info (dict): Task information from original task definition

        Returns:
            dict: Response from start_task API
        """
        task_definition = task_info['taskDefinitionArn']
        container_instance_arn = task_info['containerInstanceArn']
        tags = task_info.get('tags', [])
        overrides = task_info.get('overrides', {})

        logger.info(f"[TASK_START_REQUEST] Starting task with definition {task_definition} on instance {container_instance_arn}")

        # Run the task on the specified container instance
        response = self.client.start_task(
            cluster=self.cluster_name,
            taskDefinition=task_definition,
            startedBy='ecs-instance-monitor-lambda',
            containerInstances=[container_instance_arn],
            tags=tags,
            overrides=overrides
        )

        if 'tasks' in response and response['tasks']:
            task_arn = response['tasks'][0]['taskArn']
            task_id = task_arn.split('/')[-1]
            logger.info(f"[TASK_STARTED] Task {task_id} started on instance {container_instance_arn}")
        else:
            logger.warning(f"[TASK_START_FAILED] Failed to start task on instance {container_instance_arn}")

        return response

    @error_handler
    def describe_task(self, task_id):
        """
        Get information about a task.

        Args:
            task_id (str): Task ID

        Returns:
            dict: Task information
        """
        logger.info(f"[TASK_DESCRIBE] Getting information for task {task_id}")
        response = self.client.describe_tasks(
            cluster=self.cluster_name,
            tasks=[task_id]
        )

        if not response.get('tasks'):
            logger.warning(f"[TASK_DESCRIBE_EMPTY] No task information found for task ID {task_id}")
            return None

        logger.info(f"[TASK_DESCRIBE_SUCCESS] Retrieved information for task {task_id}")
        return response['tasks'][0]
