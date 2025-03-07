import json
import boto3
import os
import logging
from boto3.dynamodb.conditions import Attr
from functools import wraps

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def error_handler(func):
    """
    Decorator for consistent error handling across functions.

    Args:
        func: The function to wrap with error handling

    Returns:
        The wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

class Config:
    """Centralized configuration management"""

    def __init__(self):
        """Initialize configuration from environment variables"""
        self.task_table_name = os.environ.get('TASK_TABLE_NAME')
        self.job_table_name = os.environ.get('JOB_TABLE_NAME')
        self.node_table_name = os.environ.get('NODE_TABLE_NAME')
        self.ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')
        self.dcgm_health_check_task = os.environ.get('DCGM_HEALTH_CHECK_TASK')

        # Validate required configuration
        missing = []
        if not self.task_table_name: missing.append('TASK_TABLE_NAME')
        if not self.job_table_name: missing.append('JOB_TABLE_NAME')
        if not self.node_table_name: missing.append('NODE_TABLE_NAME')
        if not self.ecs_cluster_name: missing.append('ECS_CLUSTER_NAME')
        if not self.dcgm_health_check_task: missing.append('DCGM_HEALTH_CHECK_TASK')

        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")

class DynamoDBService:
    """Service for DynamoDB operations"""

    def __init__(self, task_table_name, job_table_name, node_table_name):
        """
        Initialize DynamoDB service.

        Args:
            task_table_name (str): Task table name
            job_table_name (str): Job table name
            node_table_name (str): Node table name
        """
        self.dynamodb = boto3.resource('dynamodb')
        self.task_table = self.dynamodb.Table(task_table_name)
        self.job_table = self.dynamodb.Table(job_table_name)
        self.node_table = self.dynamodb.Table(node_table_name)

    @error_handler
    def get_task(self, task_id):
        """
        Get task information by task ID.

        Args:
            task_id (str): ECS task ID

        Returns:
            dict: Task record if found, None if not found
        """
        response = self.task_table.get_item(
            Key={'ecs_task_id': task_id}
        )

        if 'Item' in response:
            return response['Item']
        else:
            logger.warning(f"No task found for task ID: {task_id}")
            return None

    @error_handler
    def get_job(self, job_id):
        """
        Get job information by job ID.

        Args:
            job_id (str): Job ID

        Returns:
            dict: Job record if found, None if not found
        """
        response = self.job_table.get_item(
            Key={'job_id': job_id}
        )

        if 'Item' in response:
            return response['Item']
        else:
            logger.warning(f"No job found for job ID: {job_id}")
            return None

    @error_handler
    def get_job_by_task_id(self, task_id):
        """
        Get job information by task ID.

        Args:
            task_id (str): ECS task ID

        Returns:
            tuple: (job_id, job_record, task_records) if found, (None, None, []) if not found
        """
        # Get task record
        task_record = self.get_task(task_id)

        if not task_record:
            return None, None, []

        job_id = task_record.get('job_id')
        if not job_id:
            logger.warning(f"No job ID found in task record for task ID: {task_id}")
            return None, None, []

        # Get job record
        job_record = self.get_job(job_id)
        if not job_record:
            return job_id, None, []

        # Get all tasks for this job
        task_records = self.task_table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        ).get('Items', [])

        logger.info(f"Found job ID: {job_id} for task ID: {task_id} with {len(task_records)} tasks")

        return job_id, job_record, task_records

    @error_handler
    def update_task_status(self, task_id, status):
        """
        Update status for a task.

        Args:
            task_id (str): ECS task ID
            status (str): Status to update to

        Returns:
            bool: True if successful, False otherwise
        """
        import datetime

        self.task_table.update_item(
            Key={'ecs_task_id': task_id},
            UpdateExpression='SET task_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated task {task_id} status to {status}")
        return True

    @error_handler
    def update_job_status(self, job_id, status):
        """
        Update status for a job.

        Args:
            job_id (str): Job ID
            status (str): Status to update to

        Returns:
            bool: True if successful, False otherwise
        """
        import datetime

        self.job_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated job {job_id} status to {status}")
        return True

    @error_handler
    def update_node_status(self, node_name, status):
        """
        Update status for a node.

        Args:
            node_name (str): Node name
            status (str): Status to update to

        Returns:
            bool: True if successful, False otherwise
        """
        import datetime

        self.node_table.update_item(
            Key={'node_name': node_name},
            UpdateExpression='SET node_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated node {node_name} status to {status}")
        return True

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
        logger.info(f"Stopping task {task_id}")
        response = self.client.stop_task(
            cluster=self.cluster_name,
            task=task_id
        )
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
        for task_id in task_ids:
            self.stop_task(task_id)
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
        logger.info(f"Set instance {container_instance_arn} status to {status}")
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
        logger.info(f"Running DCGM health check task on {container_instance_arn}")

        # Set instance status to PENDING_HEALTHCHECK
        self.set_instance_status(container_instance_arn, 'PENDING_HEALTHCHECK', cluster_arn)

        # Run DCGM health check task
        response = self.client.start_task(
            cluster=cluster_arn,
            taskDefinition=dcgm_task_def,
            containerInstances=[container_instance_arn],
            tags=[{
                'key': 'job_id',
                'value': job_id
            }]
        )
        logger.info(f"DCGM health check task response: {response}")
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
        response = self.client.describe_tasks(
            cluster=self.cluster_name,
            tasks=[task_id]
        )

        if not response.get('tasks'):
            logger.warning(f"No task information found for task ID {task_id}")
            return None

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
            return []

        # Use the first task ID to query
        tasks_response = self.client.describe_tasks(
            cluster=self.cluster_name,
            tasks=[task_ids[0]]
        )

        container_instance_arns = []
        for task in tasks_response.get('tasks', []):
            if task.get('containerInstanceArn') and task['containerInstanceArn'] not in container_instance_arns:
                container_instance_arns.append(task['containerInstanceArn'])

        return container_instance_arns

class TaskProcessor:
    """Processor for task-related operations"""

    def __init__(self, db_service, ecs_service, config):
        """
        Initialize task processor.

        Args:
            db_service (DynamoDBService): DynamoDB service instance
            ecs_service (ECSService): ECS service instance
            config (Config): Configuration
        """
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.config = config

    def process_task_state_change(self, detail):
        """
        Process ECS Task State Change event.

        Args:
            detail (dict): Event detail

        Returns:
            bool: True if processed successfully, False otherwise
        """
        # Check if task is STOPPED
        if detail.get('lastStatus') != 'STOPPED':
            logger.info(f"Task status is {detail.get('lastStatus')}, no action needed")
            return False

        task_id = detail['taskArn'].split('/')[2]
        cluster_arn = detail['clusterArn']
        cluster_name = cluster_arn.split('/')[1]

        logger.info(f"Processing stopped task {task_id} in cluster {cluster_name}")

        # Get task detail
        task_detail = self.ecs_service.describe_task(task_id)
        if not task_detail:
            logger.error(f"Could not get details for task {task_id}")
            return False

        stop_code = task_detail.get('stopCode')

        # Check if task was stopped by user
        if stop_code and stop_code.startswith('UserInitiated'):
            return self.handle_user_stopped_task(task_id)
        else:
            # Get exit code from containers
            exit_code = self._get_container_exit_code(task_detail)
            logger.info(f"Task exit code: {exit_code}")

            if exit_code == 1:
                return self.handle_task_exit_code_1(task_id, cluster_arn, task_detail)
            elif exit_code == 0:
                return self.handle_task_exit_code_0(task_id)
            else:
                logger.info(f"Unhandled exit code: {exit_code}, no action taken")
                return False

    def _get_container_exit_code(self, task_detail):
        """
        Get exit code from task containers.

        Args:
            task_detail (dict): Task detail

        Returns:
            int or None: Exit code if found, None otherwise
        """
        containers = task_detail.get('containers', [])
        for container in containers:
            if container.get('exitCode') is not None:
                return container.get('exitCode')
        return None

    def handle_user_stopped_task(self, task_id):
        """
        Handle task stopped by user.

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info("Task was stopped by user")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            return False

        # Update task status
        self.db_service.update_task_status(task_id, 'USER_STOPPED')

        # Check if all tasks for this job are stopped
        all_stopped = True
        for task in task_records:
            if task.get('ecs_task_id') != task_id and task.get('task_status') != 'USER_STOPPED':
                all_stopped = False
                break

        # If all tasks are stopped, update job status
        if all_stopped and job_record:
            self.db_service.update_job_status(job_id, 'USER_STOPPED')

        return True

    def handle_task_exit_code_0(self, task_id):
        """
        Handle task with exit code 0 (success).

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info("Task exited with code 0")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            return False

        # Update task status
        self.db_service.update_task_status(task_id, 'COMPLETE')

        # Check if all tasks for this job are complete
        all_complete = True
        for task in task_records:
            if task.get('ecs_task_id') != task_id and task.get('task_status') != 'COMPLETE':
                all_complete = False
                break

        # If all tasks are complete, update job status
        if all_complete and job_record:
            self.db_service.update_job_status(job_id, 'COMPLETE')

        return True

    def handle_task_exit_code_1(self, task_id, cluster_arn, task_detail):
        """
        Handle task with exit code 1 (error).

        Args:
            task_id (str): ECS task ID
            cluster_arn (str): ECS cluster ARN
            task_detail (dict): Task detail

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info("Task exited with code 1")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            return False

        # Update task status
        self.db_service.update_task_status(task_id, 'FAILED')

        # Get all task IDs for this job
        task_ids = self._extract_task_ids(task_records)

        # Stop all tasks for this job
        self.ecs_service.stop_tasks(task_ids)

        # Set related instances to PENDING
        self._set_related_instances_pending(task_records, task_detail, cluster_arn)

        # Get container instance ARNs
        container_instance_arns = self.ecs_service.get_container_instances_from_tasks(task_ids)

        # Run GPU DCGM check task on each container instance
        for container_instance_arn in container_instance_arns:
            # Get node name from task records
            node_name = None
            for task in task_records:
                if task.get('container_instance_arn') == container_instance_arn:
                    node_name = task.get('node_name')
                    break

            # Run DCGM health check
            self.ecs_service.run_dcgm_health_check(
                cluster_arn,
                container_instance_arn,
                job_id,
                self.config.dcgm_health_check_task
            )

            # Update node status if node name is found
            if node_name:
                self.db_service.update_node_status(node_name, 'PENDING_HEALTHCHECK')

        # Update job status
        if job_record:
            self.db_service.update_job_status(job_id, 'FAILED')

        return True

    def _extract_task_ids(self, task_records):
        """
        Extract task IDs from task records.

        Args:
            task_records (list): List of task records

        Returns:
            list: List of task IDs
        """
        task_ids = []
        for record in task_records:
            task_id = record.get('ecs_task_id')
            if task_id:
                task_ids.append(task_id)
        return task_ids

    def _set_related_instances_pending(self, task_records, task_detail, cluster_arn):
        """
        Set related instances to PENDING status.

        Args:
            task_records (list): List of task records
            task_detail (dict): Task detail
            cluster_arn (str): Cluster ARN

        Returns:
            bool: True if successful
        """
        current_instance_arn = task_detail.get('containerInstanceArn')

        for record in task_records:
            container_instance_arn = record.get('container_instance_arn')
            node_name = record.get('node_name')

            if container_instance_arn and container_instance_arn != current_instance_arn:
                # Update ECS container instance status
                self.ecs_service.set_instance_status(
                    container_instance_arn,
                    'PENDING',
                    cluster_arn
                )

                # Update node status in DynamoDB
                if node_name:
                    self.db_service.update_node_status(node_name, 'PENDING')

        return True

def validate_ecs_task_event(event):
    """
    Validates that the event is an ECS event.

    Args:
        event (dict): The event to validate

    Returns:
        bool: True if valid event, False otherwise
    """
    if event.get("source") != "aws.ecs":
        logger.error("Function only supports input from events with a source type of: aws.ecs")
        return False

    return True

def lambda_handler(event, context):
    """
    Lambda handler for ECS task events directly from EventBridge

    Args:
        event (dict): Lambda event from EventBridge
        context (LambdaContext): Lambda context

    Returns:
        dict: Response
    """
    logger.info('Event received: %s', json.dumps(event))

    # Initialize configuration
    config = Config()

    # Validate event
    if not validate_ecs_task_event(event):
        return {
            'statusCode': 400,
            'body': 'Function only supports input from events with a source type of: aws.ecs'
        }

    # Handle ECS Task State Change
    if event["detail-type"] == "ECS Task State Change":
        db_service = DynamoDBService(
            config.task_table_name,
            config.job_table_name,
            config.node_table_name
        )
        ecs_service = ECSService(config.ecs_cluster_name)
        processor = TaskProcessor(db_service, ecs_service, config)
        processor.process_task_state_change(event["detail"])

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
