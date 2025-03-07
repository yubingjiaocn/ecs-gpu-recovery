import boto3
import logging
import datetime
from boto3.dynamodb.conditions import Attr
from utils import error_handler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
