import boto3
from common import logger, error_handler

class ECSService:
    """Service for ECS operations"""

    def __init__(self, cluster_name):
        """
        Initialize ECS service.

        Args:
            cluster_name (str): ECS cluster name
        """
        self.cluster_name = cluster_name
        self.client = boto3.client('ecs')

    @error_handler
    def stop_task(self, task_id):
        """
        Stop a task.

        Args:
            task_id (str): Task ID to stop

        Returns:
            dict: Response from stop_task API
        """
        logger.info(f"[TASK_STOP_REQUEST] Stopping task {task_id}")
        response = self.client.stop_task(
            cluster=self.cluster_name,
            task=task_id
        )
        logger.info(f"[TASK_STOP_RESPONSE] Task {task_id} stop request sent")
        return response

    @error_handler
    def stop_tasks(self, task_ids):
        """
        Stop multiple tasks.

        Args:
            task_ids (list): List of task IDs to stop

        Returns:
            bool: True if all tasks were stopped successfully
        """
        logger.info(f"[TASK_STOP_BATCH] Stopping {len(task_ids)} tasks")
        for task_id in task_ids:
            self.stop_task(task_id)
        logger.info(f"[TASK_STOP_BATCH_COMPLETE] Stop requests sent for {len(task_ids)} tasks")
        return True

    @error_handler
    def set_instance_status(self, container_instance_arn, status, cluster_arn=None):
        """
        Set the status attribute of a container instance.

        Args:
            container_instance_arn (str): Container instance ARN
            status (str): Status value to set
            cluster_arn (str, optional): Cluster ARN. Defaults to None.

        Returns:
            dict: Response from put_attributes API
        """
        cluster = cluster_arn if cluster_arn else self.cluster_name
        logger.info(f"[ATTRIBUTE_CHANGE] Setting instance {container_instance_arn} status to {status}")
        response = self.client.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    'name': 'status',
                    'value': status,
                    'targetType': 'container-instance',
                    'targetId': container_instance_arn
                }
            ]
        )
        logger.info(f"[ATTRIBUTE_CHANGE_COMPLETE] Set instance {container_instance_arn} status to {status}")
        return response

    @error_handler
    def run_dcgm_health_check(self, cluster_arn, container_instance_arn, job_id, dcgm_task_def):
        """
        Run DCGM health check task on a container instance.

        Args:
            cluster_arn (str): ECS cluster ARN
            container_instance_arn (str): Container instance ARN
            job_id (str): Job ID
            dcgm_task_def (str): DCGM health check task definition

        Returns:
            dict: ECS start_task response
        """
        logger.info(f"[HEALTH_CHECK_START] Running DCGM health check task on {container_instance_arn} for job {job_id}")

        # Set instance status to PENDING_HEALTHCHECK
        self.set_instance_status(container_instance_arn, 'PENDING_HEALTHCHECK', cluster_arn)

        # Run DCGM health check task
        response = self.client.start_task(
            cluster=cluster_arn,
            taskDefinition=dcgm_task_def,
            startedBy='ecs-task-handler-lambda',
            containerInstances=[container_instance_arn],
            tags=[{
                'key': 'job_id',
                'value': job_id
            }]
        )

        if 'tasks' in response and response['tasks']:
            task_arn = response['tasks'][0]['taskArn']
            task_id = task_arn.split('/')[-1]
            logger.info(f"[HEALTH_CHECK_STARTED] DCGM health check task {task_id} started on {container_instance_arn}")
        else:
            logger.warning(f"[HEALTH_CHECK_FAILED] Failed to start DCGM health check task on {container_instance_arn}")

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

    @error_handler
    def get_container_instances_from_tasks(self, task_ids):
        """
        Get container instance ARNs from task IDs.

        Args:
            task_ids (list): List of task IDs

        Returns:
            list: List of unique container instance ARNs
        """
        if not task_ids:
            logger.warning("[TASK_QUERY_EMPTY] Empty task ID list provided")
            return []

        logger.info(f"[TASK_QUERY] Getting container instances for {len(task_ids)} tasks")
        # Use the first task ID to query
        tasks_response = self.client.describe_tasks(
            cluster=self.cluster_name,
            tasks=[task_ids[0]]
        )

        container_instance_arns = []
        for task in tasks_response.get('tasks', []):
            if task.get('containerInstanceArn') and task['containerInstanceArn'] not in container_instance_arns:
                container_instance_arns.append(task['containerInstanceArn'])

        logger.info(f"[TASK_QUERY_SUCCESS] Found {len(container_instance_arns)} container instances")
        return container_instance_arns
