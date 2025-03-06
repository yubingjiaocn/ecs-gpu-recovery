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
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        self.ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')

        # Validate required configuration
        missing = []
        if not self.training_job_table_name: missing.append('TRAINING_JOB_TABLE_NAME')
        if not self.sns_topic_arn: missing.append('SNS_TOPIC_ARN')
        if not self.ecs_cluster_name: missing.append('ECS_CLUSTER_NAME')

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
    def get_jobs_by_instance_id(self, instance_id):
        """
        Query jobs associated with a container instance ID.

        Args:
            instance_id (str): Container instance ID

        Returns:
            list: List of job records associated with the instance
        """
        response = self.table.scan(
            FilterExpression=Attr('container_inst_id').eq(instance_id)
        )
        return response.get('Items', [])

    @error_handler
    def get_tasks_by_job_id(self, job_id):
        """
        Query all tasks associated with a job ID.

        Args:
            job_id (str): Job ID

        Returns:
            list: List of job records associated with the job ID
        """
        response = self.table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        )
        return response.get('Items', [])

    @error_handler
    def update_job_status(self, job_id_rank, status):
        """
        Update the status of a job in DynamoDB.

        Args:
            job_id_rank (str): Job ID rank (primary key)
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
        self.table.update_item(
            Key={'job_id_rank': job_id_rank},
            UpdateExpression='SET job_status = :val',
            ExpressionAttributeValues={':val': status}
        )
        logger.info(f"Updated job {job_id_rank} status to {status}")
        return True

    @error_handler
    def update_job_retry(self, job_id_rank, retry, new_task_id):
        """
        Update the retry count and task ID of a job in DynamoDB.

        Args:
            job_id_rank (str): Job ID rank (primary key)
            retry (int): New retry value
            new_task_id (str): New ECS Task ID

        Returns:
            bool: True if successful, False otherwise
        """
        self.table.update_item(
            Key={'job_id_rank': job_id_rank},
            UpdateExpression='SET retry = :rt, ecs_task_id = :tid, #s = :s',
            ExpressionAttributeNames={
                '#s': 'status'
            },
            ExpressionAttributeValues={
                ':rt': str(retry),
                ':tid': new_task_id,
                ':s': 'RUNNING'
            }
        )
        logger.info(f"Updated job {job_id_rank} retry to {retry} and task ID to {new_task_id}")
        return True

    @error_handler
    def update_all_job_statuses(self, job_id, status):
        """
        Update the status of all records for a job ID in DynamoDB.
        Uses batch processing for efficiency.

        Args:
            job_id (str): Job ID
            status (str): New status value

        Returns:
            bool: True if successful, False otherwise
        """
        # Get all records for this job ID
        job_records = self.get_tasks_by_job_id(job_id)

        if not job_records:
            logger.warning(f"No records found for job ID {job_id}")
            return False

        # Process in batches of 25 (DynamoDB batch limit)
        for i in range(0, len(job_records), 25):
            batch = job_records[i:i+25]
            for record in batch:
                job_id_rank = record.get('job_id_rank')
                if job_id_rank:
                    self.update_job_status(job_id_rank, status)

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
    def set_instance_status(self, container_instance_arn, status):
        """
        Set the status attribute of a container instance.

        Args:
            container_instance_arn (str): Container instance ARN
            status (str): Status value to set

        Returns:
            dict: Response from put_attributes API
        """
        response = self.client.put_attributes(
            cluster=self.cluster_name,
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
    def run_training_task(self, task_info):
        """
        Re-execute a training task on a container instance.

        Args:
            task_info (dict): Task information from original task definition

        Returns:
            dict: Response from start_task API
        """
        task_definition = task_info['taskDefinitionArn']
        container_instance_arn = task_info['containerInstanceArn']
        tags = task_info.get('tags', [])
        overrides = task_info.get('overrides', {})

        # Run the task on the specified container instance
        response = self.client.start_task(
            cluster=self.cluster_name,
            taskDefinition=task_definition,
            startedBy='ecs-instance-monitor-lambda',
            containerInstances=[container_instance_arn],
            tags=tags,
            overrides=overrides
        )

        logger.info(f"Started training task: {response}")
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

class NotificationService:
    """Service for SNS notifications"""

    def __init__(self, sns_topic_arn):
        """
        Initialize notification service.

        Args:
            sns_topic_arn (str): SNS topic ARN
        """
        self.sns_topic_arn = sns_topic_arn
        self.client = boto3.client('sns')

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
        response = self.client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"Sent notification: {subject}")
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
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.notification_service = notification_service

    def process_job(self, job, instance_id):
        """
        Process a job based on its current state.

        Args:
            job (dict): Job information
            instance_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        job_id = job.get('job_id')
        job_status = job.get('job_status')
        retry = int(job.get('retry', 0))

        logger.info(f"Processing job: {job_id}, status: {job_status}, retry: {retry}")

        # Skip if job status is 'FAILED'
        if job_status == 'FAILED':
            logger.info(f"Job {job_id} status is 'FAILED', skipping")
            return False

        # Process based on retry count
        if retry == 0:
            return self._handle_first_retry(job, instance_id)
        else:
            return self._handle_subsequent_retry(job, instance_id)

    def _handle_first_retry(self, job, instance_id):
        """
        Handle first retry attempt for a job.

        Args:
            job (dict): Job information
            instance_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        job_id = job.get('job_id')
        logger.info(f"Job {job_id} retry is 0, re-executing all related training tasks")

        # Get all tasks associated with this job ID
        job_tasks = self.db_service.get_tasks_by_job_id(job_id)

        if not job_tasks:
            logger.warning(f"No tasks found for job ID {job_id}")
            return False

        logger.info(f"Found {len(job_tasks)} tasks for job ID {job_id}")

        successful_tasks = 0

        # Run each task
        for task in job_tasks:
            task_id = task.get('ecs_task_id')
            container_inst_arn = task.get('containerInstanceArn')
            job_id_rank = task.get('job_id_rank')

            if not task_id:
                logger.warning(f"No task ID found for job record {job_id_rank}")
                continue

            # Get task information
            task_info = self.ecs_service.describe_task(task_id)
            if not task_info:
                continue

            # Set container instance status to INPROGRESS for retry
            if container_inst_arn:
                self.ecs_service.set_instance_status(container_inst_arn, 'INPROGRESS')

            # Run the task
            response = self.ecs_service.run_training_task(task_info)

            if response:
                # Update task information in DynamoDB
                new_task_id = response['tasks'][0]["taskArn"].split('/')[-1]
                self.db_service.update_job_retry(job_id_rank, 1, new_task_id)
                successful_tasks += 1

        logger.info(f"Successfully restarted {successful_tasks} tasks for job {job_id}")
        return successful_tasks > 0

    def _handle_subsequent_retry(self, job, instance_id):
        """
        Handle subsequent retry attempts for a job.

        Args:
            job (dict): Job information
            instance_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        job_id = job.get('job_id')
        logger.info(f"Job {job_id} retry is not 0, updating all related tasks to 'fail'")

        # Update all job statuses to 'FAILED'
        self.db_service.update_all_job_statuses(job_id, 'FAILED')

        # Get all tasks for this job to handle instance attributes
        job_tasks = self.db_service.get_tasks_by_job_id(job_id)

        for task in job_tasks:
            container_inst_arn = task.get('containerInstanceArn')
            if container_inst_arn:
                if task.get('container_inst_id') == instance_id:
                    # Mark failed instance as FAILED
                    self.ecs_service.set_instance_status(container_inst_arn, 'FAILED')
                else:
                    # Release other related instances to AVAILABLE
                    self.ecs_service.set_instance_status(container_inst_arn, 'AVAILABLE')

        # Send notification to technical staff
        subject = f"Job {job_id} failed after instance restart"
        message = (f"Job {job_id} on instance {instance_id} failed after restart. "
                  f"All related tasks have been marked as failed. Please investigate.")
        self.notification_service.send_notification(subject, message)

        return True

def validate_ecs_container_instance_event(event):
    """
    Validates that the event is an ECS Container Instance State Change event.

    Args:
        event (dict): The event to validate

    Returns:
        bool: True if valid event, False otherwise
    """
    if event.get("source") != "aws.ecs":
        logger.error("Function only supports input from events with a source type of: aws.ecs")
        return False

    if event.get("detail-type") != "ECS Container Instance State Change":
        logger.error("Function only supports ECS Container Instance State Change events")
        return False

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
    logger.info(f"Processing restarted instance {instance_id}")

    # Initialize services
    db_service = DynamoDBService(config.training_job_table_name)
    ecs_service = ECSService(config.ecs_cluster_name)
    notification_service = NotificationService(config.sns_topic_arn)
    job_processor = JobProcessor(db_service, ecs_service, notification_service)

    # Set instance status to AVAILABLE
    ecs_service.set_instance_status(detail['containerInstanceArn'], 'AVAILABLE')

    # Query training jobs associated with this instance
    jobs = db_service.get_jobs_by_instance_id(instance_id)

    if not jobs:
        logger.info(f"No jobs found for instance {instance_id}")
        return {
            'statusCode': 200,
            'body': f"No jobs found for instance {instance_id}"
        }

    processed_jobs = 0

    # Process each job
    for job in jobs:
        if job_processor.process_job(job, instance_id):
            processed_jobs += 1

    return {
        'statusCode': 200,
        'body': f"Processed {processed_jobs} jobs"
    }

def lambda_handler(event, context):
    """
    Lambda handler for ECS Container Instance State Change events.
    Monitors instances being restarted and handles related training jobs.

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
    if not validate_ecs_container_instance_event(event):
        return {
            'statusCode': 400,
            'body': 'Invalid event format'
        }

    detail = event["detail"]

    # Extract instance ID
    instance_id = detail.get('containerInstanceArn', '')
    if not instance_id:
        logger.error("No instance ID in event")
        return {
            'statusCode': 400,
            'body': 'No instance ID in event'
        }

    instance_id = instance_id.split('/')[-1]

    # Check if this is an instance starting up (ACTIVE status)
    if detail.get('status') != 'ACTIVE':
        logger.info(f"Container instance status is {detail.get('status')}, no action needed")
        return {
            'statusCode': 200,
            'body': f"Container instance status is {detail.get('status')}, no action needed"
        }

    # Process the active instance
    return process_active_instance(instance_id, detail, config)
