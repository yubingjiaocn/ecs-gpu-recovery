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
        self.training_job_table_name = os.environ.get('TRAINING_JOB_TABLE_NAME')
        self.ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')
        self.dcgm_health_check_task = os.environ.get('DCGM_HEALTH_CHECK_TASK')

        # Validate required configuration
        missing = []
        if not self.training_job_table_name: missing.append('TRAINING_JOB_TABLE_NAME')
        if not self.ecs_cluster_name: missing.append('ECS_CLUSTER_NAME')
        if not self.dcgm_health_check_task: missing.append('DCGM_HEALTH_CHECK_TASK')

        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")

class DynamoDBService:
    """Service for DynamoDB operations"""

    def __init__(self, table_name):
        """
        Initialize DynamoDB service.

        Args:
            table_name (str): DynamoDB table name
        """
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    @error_handler
    def get_job_by_task_id(self, task_id):
        """
        Get job information by task ID.

        Args:
            task_id (str): ECS task ID

        Returns:
            tuple: (job_id, job_records) if found, (None, None) if not found
        """
        response = self.table.scan(
            FilterExpression=Attr('ecs_task_id').eq(task_id)
        )

        if response.get('Items'):
            job_id = response['Items'][0]['job_id']
            logger.info(f"Found job ID: {job_id} for task ID: {task_id}")

            # Get all records for this job
            job_records = self.table.scan(
                FilterExpression=Attr('job_id').eq(job_id)
            )

            return job_id, job_records.get('Items', [])
        else:
            logger.warning(f"No job found for task ID: {task_id}")
            return None, None

    @error_handler
    def update_job_status(self, job_records, status):
        """
        Update status for all records associated with a job.

        Args:
            job_records (list): List of job records
            status (str): Status to update to

        Returns:
            bool: True if successful, False otherwise
        """
        for record in job_records:
            job_id_rank = record.get('job_id_rank')
            if job_id_rank:
                self.table.update_item(
                    Key={'job_id_rank': job_id_rank},
                    UpdateExpression='SET #s = :val',
                    ExpressionAttributeNames={
                        '#s': 'status'
                    },
                    ExpressionAttributeValues={':val': status}
                )
                logger.info(f"Updated job_id_rank {job_id_rank} status to {status}")
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
                    'value': status
                }
            ],
            targetId=container_instance_arn
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

        job_id, job_records = self.db_service.get_job_by_task_id(task_id)
        if job_id and job_records:
            self.db_service.update_job_status(job_records, 'Other')
            return True
        return False

    def handle_task_exit_code_0(self, task_id):
        """
        Handle task with exit code 0 (success).

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info("Task exited with code 0")

        job_id, job_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id or not job_records:
            return False

        # Check current status
        current_status = job_records[0].get('status')
        if current_status == 'complete':
            logger.info("Job status is already complete, exiting")
            return True

        if current_status == 'inprogress':
            # Update status to 'complete'
            self.db_service.update_job_status(job_records, 'complete')
            return True

        return False

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

        job_id, job_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id or not job_records:
            return False

        # Get all task IDs for this job
        task_ids = self._extract_task_ids(job_records)

        # Stop all tasks for this job
        self.ecs_service.stop_tasks(task_ids)

        # Set related instances to PENDING
        self._set_related_instances_pending(job_records, task_detail, cluster_arn)

        # Get container instance ARNs
        container_instance_arns = self.ecs_service.get_container_instances_from_tasks(task_ids)

        # Run GPU DCGM check task on each container instance
        for container_instance_arn in container_instance_arns:
            self.ecs_service.run_dcgm_health_check(
                cluster_arn,
                container_instance_arn,
                job_id,
                self.config.dcgm_health_check_task
            )

        # Update status to 'STOPPED' for all records with this job ID
        self.db_service.update_job_status(job_records, 'STOPPED')
        return True

    def _extract_task_ids(self, job_records):
        """
        Extract task IDs from job records.

        Args:
            job_records (list): List of job records

        Returns:
            list: List of task IDs
        """
        task_ids = []
        for record in job_records:
            task_id = record.get('ecs_task_id')
            if task_id:
                task_ids.append(task_id)
        return task_ids

    def _set_related_instances_pending(self, job_records, task_detail, cluster_arn):
        """
        Set related instances to PENDING status.

        Args:
            job_records (list): List of job records
            task_detail (dict): Task detail
            cluster_arn (str): Cluster ARN

        Returns:
            bool: True if successful
        """
        current_instance_arn = task_detail.get('containerInstanceArn')

        for record in job_records:
            container_instance_arn = record.get('containerInstanceArn')
            if container_instance_arn and container_instance_arn != current_instance_arn:
                self.ecs_service.set_instance_status(
                    container_instance_arn,
                    'PENDING',
                    cluster_arn
                )

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
        db_service = DynamoDBService(config.training_job_table_name)
        ecs_service = ECSService(config.ecs_cluster_name)
        processor = TaskProcessor(db_service, ecs_service, config)
        processor.process_task_state_change(event["detail"])

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
