import json
import boto3
import os
import logging
import datetime
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
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        self.ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')

        # Validate required configuration
        missing = []
        if not self.task_table_name: missing.append('TASK_TABLE_NAME')
        if not self.job_table_name: missing.append('JOB_TABLE_NAME')
        if not self.node_table_name: missing.append('NODE_TABLE_NAME')
        if not self.sns_topic_arn: missing.append('SNS_TOPIC_ARN')
        if not self.ecs_cluster_name: missing.append('ECS_CLUSTER_NAME')

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
    def get_tasks_by_container_instance_id(self, container_inst_id):
        """
        Query tasks associated with a container instance ID.

        Args:
            container_inst_id (str): Container instance ID

        Returns:
            list: List of task records associated with the instance
        """
        logger.info(f"[DB_QUERY] Getting tasks for container instance ID: {container_inst_id}")
        response = self.task_table.scan(
            FilterExpression=Attr('container_inst_id').eq(container_inst_id)
        )
        tasks = response.get('Items', [])
        logger.info(f"[DB_QUERY_RESULT] Found {len(tasks)} tasks for container instance ID: {container_inst_id}")
        return tasks

    @error_handler
    def get_tasks_by_job_id(self, job_id):
        """
        Query all tasks associated with a job ID.

        Args:
            job_id (str): Job ID

        Returns:
            list: List of task records associated with the job ID
        """
        logger.info(f"[DB_QUERY] Getting tasks for job ID: {job_id}")
        response = self.task_table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        )
        tasks = response.get('Items', [])
        logger.info(f"[DB_QUERY_RESULT] Found {len(tasks)} tasks for job ID: {job_id}")
        return tasks

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
    def update_task_status(self, task_id, status):
        """
        Update the status of a task in DynamoDB.

        Args:
            task_id (str): ECS task ID
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
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
        Update the status of a job in DynamoDB.

        Args:
            job_id (str): Job ID
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
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
        Update the status of a node in DynamoDB.

        Args:
            node_name (str): Node name
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
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

    @error_handler
    def update_task_retry(self, task_id, retry, new_task_id):
        """
        Update the retry count and task ID of a task in DynamoDB.

        Args:
            task_id (str): Original ECS task ID
            retry (int): New retry value
            new_task_id (str): New ECS Task ID

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"[TASK_RETRY] Updating task {task_id} with retry {retry} and new task ID {new_task_id}")
        # Get the task record first to preserve other fields
        response = self.task_table.get_item(
            Key={'ecs_task_id': task_id}
        )

        if 'Item' not in response:
            logger.warning(f"[TASK_RETRY_ERROR] No task found for task ID: {task_id}")
            return False

        # Create a new task record with the new task ID
        task_record = response['Item']
        task_record['ecs_task_id'] = new_task_id
        task_record['retry'] = str(retry)
        task_record['task_status'] = 'IN_PROGRESS'
        task_record['updated_at'] = datetime.datetime.now().isoformat()

        # Put the new task record
        self.task_table.put_item(Item=task_record)
        logger.info(f"[TASK_RETRY_COMPLETE] Created new task record for {new_task_id} with retry {retry}")
        return True

    @error_handler
    def update_all_task_statuses(self, job_id, status):
        """
        Update the status of all tasks for a job ID in DynamoDB.

        Args:
            job_id (str): Job ID
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"[BATCH_UPDATE] Updating all tasks for job {job_id} to status {status}")
        # Get all tasks for this job ID
        task_records = self.get_tasks_by_job_id(job_id)

        if not task_records:
            logger.warning(f"[BATCH_UPDATE_EMPTY] No tasks found for job ID {job_id}")
            return False

        # Update each task status
        updated_count = 0
        for task in task_records:
            task_id = task.get('ecs_task_id')
            if task_id:
                self.update_task_status(task_id, status)
                updated_count += 1

        logger.info(f"[BATCH_UPDATE_COMPLETE] Updated {updated_count} tasks for job {job_id}")

        # Also update the job status
        self.update_job_status(job_id, status)

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

class NotificationService:
    """Service for SNS notifications"""

    def __init__(self, sns_topic_arn):
        """
        Initialize notification service.

        Args:
            sns_topic_arn (str): SNS topic ARN
        """
        logger.info(f"[NOTIFICATION_INIT] Initializing notification service with topic: {sns_topic_arn}")
        self.sns_topic_arn = sns_topic_arn
        self.client = boto3.client('sns')
        logger.info("[NOTIFICATION_INIT_COMPLETE] Notification service initialized")

    @error_handler
    def send_notification(self, subject, message):
        """
        Send an SNS notification.

        Args:
            subject (str): Notification subject
            message (str): Notification message

        Returns:
            dict: Response from SNS publish API
        """
        logger.info(f"[NOTIFICATION_SEND] Sending notification: {subject}")
        response = self.client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"[NOTIFICATION_SENT] Notification sent: {subject}")
        return response

class JobProcessor:
    """Processor for job-related operations"""

    def __init__(self, db_service, ecs_service, notification_service):
        """
        Initialize job processor.

        Args:
            db_service (DynamoDBService): DynamoDB service instance
            ecs_service (ECSService): ECS service instance
            notification_service (NotificationService): Notification service instance
        """
        logger.info("[PROCESSOR_INIT] Initializing job processor")
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.notification_service = notification_service
        logger.info("[PROCESSOR_INIT_COMPLETE] Job processor initialized")

    def process_job(self, job_id, container_inst_id):
        """
        Process a job based on its current state.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_PROCESS_START] Processing job {job_id} on instance {container_inst_id}")
        # Get job record
        job_record = self.db_service.get_job(job_id)
        if not job_record:
            logger.warning(f"[JOB_PROCESS_ERROR] No job record found for job ID {job_id}")
            return False

        job_status = job_record.get('job_status')
        retry = int(job_record.get('retry', 0))

        logger.info(f"[JOB_STATUS] Job: {job_id}, status: {job_status}, retry: {retry}")

        # Skip if job status is 'FAILED'
        if job_status == 'FAILED':
            logger.info(f"[JOB_SKIP] Job {job_id} status is 'FAILED', skipping")
            return False

        # Process based on retry count
        if retry == 0:
            logger.info(f"[JOB_RETRY_FIRST] Handling first retry for job {job_id}")
            return self._handle_first_retry(job_id, container_inst_id)
        else:
            logger.info(f"[JOB_RETRY_SUBSEQUENT] Handling subsequent retry for job {job_id}")
            return self._handle_subsequent_retry(job_id, container_inst_id)

    def _handle_first_retry(self, job_id, container_inst_id):
        """
        Handle first retry attempt for a job.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_RETRY_FIRST_START] Job {job_id} retry is 0, re-executing all related tasks")

        # Get all tasks associated with this job ID
        tasks = self.db_service.get_tasks_by_job_id(job_id)

        if not tasks:
            logger.warning(f"[JOB_RETRY_ERROR] No tasks found for job ID {job_id}")
            return False

        logger.info(f"[JOB_RETRY_TASKS] Found {len(tasks)} tasks for job ID {job_id}")

        successful_tasks = 0

        # Run each task
        for task in tasks:
            task_id = task.get('ecs_task_id')
            container_instance_arn = task.get('container_instance_arn')
            node_name = task.get('node_name')

            if not task_id:
                logger.warning(f"[TASK_ERROR] No task ID found for task record")
                continue

            logger.info(f"[TASK_RETRY] Retrying task {task_id} for job {job_id}")

            # Get task information
            task_info = self.ecs_service.describe_task(task_id)
            if not task_info:
                logger.warning(f"[TASK_RETRY_ERROR] Could not get task information for {task_id}")
                continue

            # Set container instance status to IN_PROGRESS for retry
            if container_instance_arn:
                logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to IN_PROGRESS")
                self.ecs_service.set_instance_status(container_instance_arn, 'IN_PROGRESS')

                # Update node status if node name is available
                if node_name:
                    logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to IN_PROGRESS")
                    self.db_service.update_node_status(node_name, 'IN_PROGRESS')

            # Run the task
            response = self.ecs_service.run_task(task_info)

            if response and response.get('tasks'):
                # Update task information in DynamoDB
                new_task_id = response['tasks'][0]["taskArn"].split('/')[-1]
                logger.info(f"[TASK_RETRY_SUCCESS] Task {task_id} restarted as {new_task_id}")
                self.db_service.update_task_retry(task_id, 1, new_task_id)
                successful_tasks += 1
            else:
                logger.warning(f"[TASK_RETRY_FAILED] Failed to restart task {task_id}")

        # Update job retry count
        if successful_tasks > 0:
            logger.info(f"[JOB_RETRY_UPDATE] Updating job {job_id} retry count to 1")
            job_record = self.db_service.get_job(job_id)
            if job_record:
                self.db_service.job_table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET retry = :rt, updated_at = :time',
                    ExpressionAttributeValues={
                        ':rt': '1',
                        ':time': datetime.datetime.now().isoformat()
                    }
                )
                logger.info(f"[JOB_RETRY_UPDATED] Job {job_id} retry count updated to 1")

        logger.info(f"[JOB_RETRY_FIRST_COMPLETE] Successfully restarted {successful_tasks} tasks for job {job_id}")
        return successful_tasks > 0

    def _handle_subsequent_retry(self, job_id, container_inst_id):
        """
        Handle subsequent retry attempts for a job.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_RETRY_SUBSEQUENT_START] Job {job_id} retry is not 0, updating all related tasks to 'FAILED'")

        # Update all task statuses to 'FAILED'
        self.db_service.update_all_task_statuses(job_id, 'FAILED')

        # Get all tasks for this job to handle instance attributes
        tasks = self.db_service.get_tasks_by_job_id(job_id)
        logger.info(f"[JOB_RETRY_TASKS] Found {len(tasks)} tasks for job {job_id}")

        for task in tasks:
            container_instance_arn = task.get('container_instance_arn')
            node_name = task.get('node_name')
            task_container_inst_id = task.get('container_inst_id')

            if container_instance_arn:
                if task_container_inst_id == container_inst_id:
                    # Mark failed instance as FAILED
                    logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to FAILED")
                    self.ecs_service.set_instance_status(container_instance_arn, 'FAILED')

                    # Update node status if node name is available
                    if node_name:
                        logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to FAILED")
                        self.db_service.update_node_status(node_name, 'FAILED')
                else:
                    # Release other related instances to AVAILABLE
                    logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to AVAILABLE")
                    self.ecs_service.set_instance_status(container_instance_arn, 'AVAILABLE')

                    # Update node status if node name is available
                    if node_name:
                        logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to AVAILABLE")
                        self.db_service.update_node_status(node_name, 'AVAILABLE')

        # Send notification to technical staff
        subject = f"Job {job_id} failed after instance restart"
        message = (f"Job {job_id} on instance {container_inst_id} failed after restart. "
                  f"All related tasks have been marked as failed. Please investigate.")
        logger.info(f"[NOTIFICATION_PREPARE] Sending failure notification for job {job_id}")
        self.notification_service.send_notification(subject, message)

        logger.info(f"[JOB_RETRY_SUBSEQUENT_COMPLETE] Job {job_id} marked as failed after retry")
        return True

def validate_ecs_container_instance_event(event):
    """
    Validates that the event is an ECS Container Instance State Change event.

    Args:
        event (dict): The event to validate

    Returns:
        bool: True if valid event, False otherwise
    """
    logger.info("[EVENT_VALIDATION] Validating event source and type")
    if event.get("source") != "aws.ecs":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports input from events with a source type of: aws.ecs")
        return False

    if event.get("detail-type") != "ECS Container Instance State Change":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports ECS Container Instance State Change events")
        return False

    logger.info("[EVENT_VALIDATION_SUCCESS] Event source and type are valid")
    return True

def process_active_instance(instance_id, detail, config):
    """
    Process an active container instance.

    Args:
        instance_id (str): Container instance ID
        detail (dict): Event detail
        config (Config): Configuration

    Returns:
        dict: Response
    """
    logger.info(f"[INSTANCE_PROCESS_START] Processing restarted instance {instance_id}")

    # Initialize services
    db_service = DynamoDBService(
        config.task_table_name,
        config.job_table_name,
        config.node_table_name
    )
    ecs_service = ECSService(config.ecs_cluster_name)
    notification_service = NotificationService(config.sns_topic_arn)
    job_processor = JobProcessor(db_service, ecs_service, notification_service)

    # Set instance status to AVAILABLE
    logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {detail['containerInstanceArn']} to AVAILABLE")
    ecs_service.set_instance_status(detail['containerInstanceArn'], 'AVAILABLE')

    # Query tasks associated with this instance
    tasks = db_service.get_tasks_by_container_instance_id(instance_id)

    if not tasks:
        logger.info(f"[INSTANCE_PROCESS_EMPTY] No tasks found for instance {instance_id}")
        return {
            'statusCode': 200,
            'body': f"No tasks found for instance {instance_id}"
        }

    # Get unique job IDs from tasks
    job_ids = set()
    for task in tasks:
        job_id = task.get('job_id')
        if job_id:
            job_ids.add(job_id)

    logger.info(f"[INSTANCE_PROCESS_JOBS] Found {len(job_ids)} jobs associated with instance {instance_id}")

    processed_jobs = 0

    # Process each job
    for job_id in job_ids:
        logger.info(f"[JOB_PROCESS] Processing job {job_id} for instance {instance_id}")
        if job_processor.process_job(job_id, instance_id):
            processed_jobs += 1
            logger.info(f"[JOB_PROCESS_SUCCESS] Successfully processed job {job_id}")
        else:
            logger.info(f"[JOB_PROCESS_SKIP] Skipped processing job {job_id}")

    logger.info(f"[INSTANCE_PROCESS_COMPLETE] Processed {processed_jobs} jobs for instance {instance_id}")
    return {
        'statusCode': 200,
        'body': f"Processed {processed_jobs} jobs"
    }

def lambda_handler(event, context):
    """
    Lambda handler for ECS Container Instance State Change events.
    Monitors instances being restarted and handles related jobs and tasks.

    Args:
        event (dict): Lambda event from EventBridge
        context (LambdaContext): Lambda context

    Returns:
        dict: Response
    """
    logger.info('[LAMBDA_START] ECS Instance Monitor invoked')
    logger.info(f'[EVENT_RECEIVED] Event: {json.dumps(event)}')

    # Initialize configuration
    config = Config()

    # Validate event
    if not validate_ecs_container_instance_event(event):
        logger.error('[VALIDATION_FAILED] Invalid event format')
        return {
            'statusCode': 400,
            'body': 'Invalid event format'
        }

    detail = event["detail"]

    # Extract instance ID
    instance_id = detail.get('containerInstanceArn', '')
    if not instance_id:
        logger.error("[EVENT_ERROR] No instance ID in event")
        return {
            'statusCode': 400,
            'body': 'No instance ID in event'
        }

    instance_id = instance_id.split('/')[-1]
    logger.info(f"[INSTANCE_ID] Extracted instance ID: {instance_id}")

    # Check if this is an instance starting up (ACTIVE status)
    if detail.get('status') != 'ACTIVE':
        logger.info(f"[INSTANCE_STATUS] Container instance status is {detail.get('status')}, no action needed")
        return {
            'statusCode': 200,
            'body': f"Container instance status is {detail.get('status')}, no action needed"
        }

    # Process the active instance
    result = process_active_instance(instance_id, detail, config)

    logger.info('[LAMBDA_COMPLETE] ECS Instance Monitor completed')
    return result
