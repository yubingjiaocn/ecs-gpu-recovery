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
            logger.error(f"[ERROR] Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

class Config:
    """Centralized configuration management"""

    def __init__(self):
        """Initialize configuration from environment variables"""
        logger.info("[CONFIG_INIT] Initializing configuration")
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
            logger.error(f"[CONFIG_ERROR] Missing required environment variables: {', '.join(missing)}")
        else:
            logger.info("[CONFIG_COMPLETE] Configuration initialized successfully")

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
        logger.info("[DB_INIT] Initializing DynamoDB service")
        self.dynamodb = boto3.resource('dynamodb')
        self.task_table = self.dynamodb.Table(task_table_name)
        self.job_table = self.dynamodb.Table(job_table_name)
        self.node_table = self.dynamodb.Table(node_table_name)
        logger.info("[DB_INIT_COMPLETE] DynamoDB service initialized")

    @error_handler
    def get_task(self, task_id):
        """
        Get task information by task ID.

        Args:
            task_id (str): ECS task ID

        Returns:
            dict: Task record if found, None if not found
        """
        logger.info(f"[DB_QUERY] Getting task information for task ID: {task_id}")
        response = self.task_table.get_item(
            Key={'ecs_task_id': task_id}
        )

        if 'Item' in response:
            logger.info(f"[DB_QUERY_SUCCESS] Found task record for task ID: {task_id}")
            return response['Item']
        else:
            logger.warning(f"[DB_QUERY_EMPTY] No task found for task ID: {task_id}")
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
        logger.info(f"[DB_QUERY] Getting job information for job ID: {job_id}")
        response = self.job_table.get_item(
            Key={'job_id': job_id}
        )

        if 'Item' in response:
            logger.info(f"[DB_QUERY_SUCCESS] Found job record for job ID: {job_id}")
            return response['Item']
        else:
            logger.warning(f"[DB_QUERY_EMPTY] No job found for job ID: {job_id}")
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
        logger.info(f"[DB_QUERY] Getting job information for task ID: {task_id}")
        # Get task record
        task_record = self.get_task(task_id)

        if not task_record:
            logger.warning(f"[DB_QUERY_EMPTY] No task record found for task ID: {task_id}")
            return None, None, []

        job_id = task_record.get('job_id')
        if not job_id:
            logger.warning(f"[DB_QUERY_INVALID] No job ID found in task record for task ID: {task_id}")
            return None, None, []

        # Get job record
        job_record = self.get_job(job_id)
        if not job_record:
            logger.warning(f"[DB_QUERY_EMPTY] No job record found for job ID: {job_id}")
            return job_id, None, []

        # Get all tasks for this job
        logger.info(f"[DB_QUERY] Getting all tasks for job ID: {job_id}")
        task_records = self.task_table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        ).get('Items', [])

        logger.info(f"[DB_QUERY_SUCCESS] Found job ID: {job_id} for task ID: {task_id} with {len(task_records)} tasks")

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

        logger.info(f"[ATTRIBUTE_CHANGE] Updating task {task_id} status to {status}")
        self.task_table.update_item(
            Key={'ecs_task_id': task_id},
            UpdateExpression='SET task_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"[ATTRIBUTE_CHANGE_COMPLETE] Updated task {task_id} status to {status}")
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

        logger.info(f"[ATTRIBUTE_CHANGE] Updating job {job_id} status to {status}")
        self.job_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"[ATTRIBUTE_CHANGE_COMPLETE] Updated job {job_id} status to {status}")
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

        logger.info(f"[ATTRIBUTE_CHANGE] Updating node {node_name} status to {status}")
        self.node_table.update_item(
            Key={'node_name': node_name},
            UpdateExpression='SET node_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"[ATTRIBUTE_CHANGE_COMPLETE] Updated node {node_name} status to {status}")
        return True

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
        logger.info("[PROCESSOR_INIT] Initializing task processor")
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.config = config
        logger.info("[PROCESSOR_INIT_COMPLETE] Task processor initialized")

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
            logger.info(f"[TASK_STATE] Task status is {detail.get('lastStatus')}, no action needed")
            return False

        task_id = detail['taskArn'].split('/')[2]
        cluster_arn = detail['clusterArn']
        cluster_name = cluster_arn.split('/')[1]

        logger.info(f"[TASK_STATE_CHANGE] Processing stopped task {task_id} in cluster {cluster_name}")

        # Get task detail
        task_detail = self.ecs_service.describe_task(task_id)
        if not task_detail:
            logger.error(f"[TASK_DETAIL_ERROR] Could not get details for task {task_id}")
            return False

        stop_code = task_detail.get('stopCode')
        logger.info(f"[TASK_STOP_CODE] Task {task_id} stop code: {stop_code}")

        # Check if task was stopped by user
        if stop_code and stop_code.startswith('UserInitiated'):
            logger.info(f"[TASK_USER_STOPPED] Task {task_id} was stopped by user")
            return self.handle_user_stopped_task(task_id)
        else:
            # Get exit code from containers
            exit_code = self._get_container_exit_code(task_detail)
            logger.info(f"[TASK_EXIT_CODE] Task {task_id} exit code: {exit_code}")

            if exit_code == 1:
                return self.handle_task_exit_code_1(task_id, cluster_arn, task_detail)
            elif exit_code == 0:
                return self.handle_task_exit_code_0(task_id)
            else:
                logger.info(f"[TASK_UNHANDLED_CODE] Unhandled exit code: {exit_code}, no action taken")
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
        logger.info(f"[TASK_STOP] Task {task_id} was stopped by user")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_STOP] No job found for task {task_id}")
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
            logger.info(f"[JOB_STATE_CHANGE] All tasks for job {job_id} are stopped, updating job status")
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
        logger.info(f"[TASK_COMPLETE] Task {task_id} exited with code 0")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_COMPLETE] No job found for task {task_id}")
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
            logger.info(f"[JOB_STATE_CHANGE] All tasks for job {job_id} are complete, updating job status")
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
        logger.info(f"[TASK_FAILED] Task {task_id} exited with code 1")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_FAILED] No job found for task {task_id}")
            return False

        # Update task status
        self.db_service.update_task_status(task_id, 'FAILED')

        # Get all task IDs for this job
        task_ids = self._extract_task_ids(task_records)
        logger.info(f"[TASK_BATCH] Found {len(task_ids)} tasks for job {job_id}")

        # Stop all tasks for this job
        logger.info(f"[TASK_BATCH_STOP] Stopping all tasks for job {job_id}")
        self.ecs_service.stop_tasks(task_ids)

        # Set related instances to PENDING
        logger.info(f"[INSTANCE_STATE_CHANGE] Setting related instances to PENDING for job {job_id}")
        self._set_related_instances_pending(task_records, task_detail, cluster_arn)

        # Get container instance ARNs
        container_instance_arns = self.ecs_service.get_container_instances_from_tasks(task_ids)
        logger.info(f"[HEALTH_CHECK_PREPARE] Running health checks on {len(container_instance_arns)} instances")

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
            logger.info(f"[JOB_STATE_CHANGE] Updating job {job_id} status to FAILED")
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
        logger.info(f"[INSTANCE_STATE_CHANGE] Current instance ARN: {current_instance_arn}")

        for record in task_records:
            container_instance_arn = record.get('container_instance_arn')
            node_name = record.get('node_name')

            if container_instance_arn and container_instance_arn != current_instance_arn:
                # Update ECS container instance status
                logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to PENDING")
                self.ecs_service.set_instance_status(
                    container_instance_arn,
                    'PENDING',
                    cluster_arn
                )

                # Update node status in DynamoDB
                if node_name:
                    logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to PENDING")
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
    logger.info("[EVENT_VALIDATION] Validating event source")
    if event.get("source") != "aws.ecs":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports input from events with a source type of: aws.ecs")
        return False

    logger.info("[EVENT_VALIDATION_SUCCESS] Event source is valid")
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
    logger.info('[LAMBDA_START] ECS Task Handler invoked')
    logger.info(f'[EVENT_RECEIVED] Event: {json.dumps(event)}')

    # Initialize configuration
    config = Config()

    # Validate event
    if not validate_ecs_task_event(event):
        logger.error('[VALIDATION_FAILED] Invalid event source')
        return {
            'statusCode': 400,
            'body': 'Function only supports input from events with a source type of: aws.ecs'
        }

    # Handle ECS Task State Change
    if event["detail-type"] == "ECS Task State Change":
        logger.info('[EVENT_PROCESSING] Processing ECS Task State Change event')
        db_service = DynamoDBService(
            config.task_table_name,
            config.job_table_name,
            config.node_table_name
        )
        ecs_service = ECSService(config.ecs_cluster_name)
        processor = TaskProcessor(db_service, ecs_service, config)
        processor.process_task_state_change(event["detail"])

    logger.info('[LAMBDA_COMPLETE] ECS Task Handler completed')
    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
