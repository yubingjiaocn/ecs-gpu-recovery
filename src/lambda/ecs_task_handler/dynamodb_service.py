import boto3
import datetime
from boto3.dynamodb.conditions import Attr
from common import logger, error_handler

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
    def update_job_task_ids(self, job_id, task_ids):
        """
        Update submitted_ecs_task_ids for a job.

        Args:
            job_id (str): Job ID
            task_ids (list): List of task IDs

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"[ATTRIBUTE_CHANGE] Updating job {job_id} submitted_ecs_task_ids to {task_ids}")
        self.job_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET submittd_ecs_task_ids = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': ','.join(task_ids) if isinstance(task_ids, list) else task_ids,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"[ATTRIBUTE_CHANGE_COMPLETE] Updated job {job_id} submitted_ecs_task_ids")
        return True
