import json
import boto3
import os
import logging
import datetime
from typing import Dict, List, Tuple, Optional, Any, Union
from boto3.dynamodb.conditions import Attr

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ecs_client = boto3.client('ecs')
ssm_client = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')

# ----- Event Validation Functions -----

def is_valid_ecs_event(event: Dict[str, Any]) -> bool:
    """
    Validates that the event is from ECS.

    Args:
        event: The Lambda event

    Returns:
        bool: True if the event is from ECS, False otherwise
    """
    if event.get("source") != "aws.ecs":
        logger.error("Function only supports input from events with a source type of: aws.ecs")
        return False
    return True

def is_task_state_change(event: Dict[str, Any]) -> bool:
    """
    Validates that the event is a task state change event.

    Args:
        event: The Lambda event

    Returns:
        bool: True if the event is a task state change event, False otherwise
    """
    if event["detail-type"] != "ECS Task State Change":
        logger.info("Not a task state change event, ignoring")
        return False
    return True

def is_stopped_task(detail: Dict[str, Any]) -> bool:
    """
    Validates that the task has stopped.

    Args:
        detail: The detail section of the event

    Returns:
        bool: True if the task has stopped, False otherwise
    """
    if detail.get('lastStatus') != 'STOPPED':
        logger.info(f"Task status is {detail.get('lastStatus')}, no action needed")
        return False
    return True

# ----- Task and Job Data Retrieval Functions -----

def get_job_id_from_task(task_id: str, cluster_name: str) -> Optional[str]:
    """
    Gets the job ID associated with a task from its tags.

    Args:
        task_id: The task ID
        cluster_name: The cluster name

    Returns:
        str: The job ID if found, None otherwise
    """
    try:
        describe_task_response = ecs_client.describe_tasks(
            cluster=cluster_name,
            tasks=[task_id],
            include=['TAGS']
        )

        if 'tasks' in describe_task_response and describe_task_response['tasks']:
            task = describe_task_response['tasks'][0]
            tags = task.get('tags', [])

            for tag in tags:
                logger.info(f"Tag: {tag['key']}, Value: {tag['value']}")
                if tag['key'] == 'job_id':
                    return tag['value']

        logger.warning(f"No job_id tag found for task {task_id}")
        return None
    except Exception as e:
        logger.error(f"Error getting job ID from task: {str(e)}")
        return None

def get_job_and_tasks(job_table, task_table, job_id: str) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Gets job and task information from DynamoDB.

    Args:
        job_table: The job DynamoDB table
        task_table: The task DynamoDB table
        job_id: The job ID

    Returns:
        tuple: (job_record, task_records) if found, (None, []) if not found
    """
    try:
        # Get job record
        job_response = job_table.get_item(
            Key={'job_id': job_id}
        )

        job_record = job_response.get('Item')
        if not job_record:
            logger.warning(f"No job record found for job_id {job_id}")
            return None, []

        # Get all tasks for this job
        task_response = task_table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        )

        task_records = task_response.get('Items', [])
        logger.info(f"Found {len(task_records)} tasks for job_id {job_id}")

        return job_record, task_records
    except Exception as e:
        logger.error(f"Error getting job data: {str(e)}")
        return None, []

def get_instance_id(cluster_arn: str, container_instance_arn: str) -> Optional[str]:
    """
    Gets the EC2 instance ID from the container instance ARN.

    Args:
        cluster_arn: The cluster ARN
        container_instance_arn: The container instance ARN

    Returns:
        str: The instance ID if found, None otherwise
    """
    try:
        response = ecs_client.describe_container_instances(
            cluster=cluster_arn,
            containerInstances=[container_instance_arn]
        )

        if 'containerInstances' in response and response['containerInstances']:
            return response['containerInstances'][0]['ec2InstanceId']

        logger.warning(f"No instance ID found for container instance {container_instance_arn}")
        return None
    except Exception as e:
        logger.error(f"Error getting instance ID: {str(e)}")
        return None

def get_node_name_from_container_instance(task_records: List[Dict[str, Any]], container_instance_arn: str) -> Optional[str]:
    """
    Gets the node name associated with a container instance ARN.

    Args:
        task_records: List of task records
        container_instance_arn: Container instance ARN

    Returns:
        str: Node name if found, None otherwise
    """
    for task in task_records:
        if task.get('container_instance_arn') == container_instance_arn:
            return task.get('node_name')
    return None

# ----- Instance Management Functions -----

def update_container_instance_status(cluster_arn: str, container_instance_arn: str, status: str) -> bool:
    """
    Updates the status attribute of a container instance.

    Args:
        cluster_arn: The cluster ARN
        container_instance_arn: The container instance ARN
        status: The new status

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        ecs_client.put_attributes(
            cluster=cluster_arn,
            attributes=[
                {
                    'name': 'status',
                    'value': status,
                    'target-type': 'container-instance',
                    'targetId': container_instance_arn
                }
            ]
        )
        logger.info(f"Updated container instance {container_instance_arn} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating container instance status: {str(e)}")
        return False

def reboot_instance(instance_id: str) -> bool:
    """
    Reboots an EC2 instance using SSM.

    Args:
        instance_id: The EC2 instance ID

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Rebooting instance {instance_id}")
        ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': ['reboot']}
        )
        logger.info(f"Reboot command sent to instance {instance_id}")
        return True
    except Exception as e:
        logger.error(f"Error executing reboot command: {str(e)}")
        return False

def update_node_status(node_table, node_name: str, status: str) -> bool:
    """
    Updates node status in DynamoDB.

    Args:
        node_table: The node DynamoDB table
        node_name: The node name
        status: The new status

    Returns:
        bool: True if successful, False otherwise
    """
    if not node_name:
        logger.warning("No node name provided")
        return False

    try:
        node_table.update_item(
            Key={'node_name': node_name},
            UpdateExpression='SET node_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated node {node_name} status to {status} in DynamoDB")
        return True
    except Exception as e:
        logger.error(f"Error updating node status in DynamoDB: {str(e)}")
        return False

# ----- Job and Task Status Management Functions -----

def update_job_status(job_table, job_id: str, status: str) -> bool:
    """
    Updates job status in DynamoDB.

    Args:
        job_table: The job DynamoDB table
        job_id: Job ID
        status: The new status

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        job_table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated job {job_id} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")
        return False

def update_task_status(task_table, task_id: str, status: str) -> bool:
    """
    Updates task status in DynamoDB.

    Args:
        task_table: The task DynamoDB table
        task_id: Task ID
        status: The new status

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        task_table.update_item(
            Key={'ecs_task_id': task_id},
            UpdateExpression='SET task_status = :val, updated_at = :time',
            ExpressionAttributeValues={
                ':val': status,
                ':time': datetime.datetime.now().isoformat()
            }
        )
        logger.info(f"Updated task {task_id} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating task status: {str(e)}")
        return False

# ----- Exit Code Handlers -----

def handle_exit_code_0(job_table, task_table, node_table, job_record: Dict[str, Any], task_records: List[Dict[str, Any]], cluster_arn: str) -> bool:
    """
    Handles a task with exit code 0 (successful DCGM run but job failed).
    Updates job status to FAILED and releases container instances.

    Args:
        job_table: The job DynamoDB table
        task_table: The task DynamoDB table
        node_table: The node DynamoDB table
        job_record: The job record
        task_records: List of task records
        cluster_arn: The cluster ARN

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("DCGM task exit code 0, updating job status to FAILED")

    try:
        job_id = job_record.get('job_id')

        # Update job status
        update_job_status(job_table, job_id, 'FAILED')

        # Update all task statuses
        for task in task_records:
            task_id = task.get('ecs_task_id')
            if task_id:
                update_task_status(task_table, task_id, 'FAILED')

        # Release container instances to AVAILABLE
        container_instances = {}
        for task in task_records:
            container_instance_arn = task.get('container_instance_arn')
            node_name = task.get('node_name')

            if container_instance_arn and container_instance_arn not in container_instances:
                container_instances[container_instance_arn] = node_name

        for container_instance_arn, node_name in container_instances.items():
            # Update ECS container instance status
            update_container_instance_status(cluster_arn, container_instance_arn, 'AVAILABLE')

            # Update node status in DynamoDB
            if node_name:
                update_node_status(node_table, node_name, 'AVAILABLE')

        return True
    except Exception as e:
        logger.error(f"Error handling exit code 0: {str(e)}")
        return False

def handle_exit_code_1(job_table, task_table, node_table, detail: Dict[str, Any], cluster_arn: str, job_record: Dict[str, Any], task_records: List[Dict[str, Any]]) -> bool:
    """
    Handles a task with exit code 1 (DCGM detected an issue).
    May reboot the instance if retry count is 0.

    Args:
        job_table: The job DynamoDB table
        task_table: The task DynamoDB table
        node_table: The node DynamoDB table
        detail: The detail section of the event
        cluster_arn: The cluster ARN
        job_record: The job record
        task_records: List of task records

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check retry to determine if reboot is needed
        retry = job_record.get('retry', 0)
        logger.info(f"DCGM task exit code 1, retry: {retry}")

        if retry == 0:
            logger.info("retry is 0, initiating instance reboot")
            container_instance_arn = detail['containerInstanceArn']

            # Get node name from task records
            node_name = get_node_name_from_container_instance(task_records, container_instance_arn)

            # Get instance ID from container instance
            instance_id = get_instance_id(cluster_arn, container_instance_arn)
            if not instance_id:
                logger.error("Failed to get container instance details")
                return False

            # Mark instance as REBOOTING
            update_container_instance_status(cluster_arn, container_instance_arn, 'REBOOTING')

            # Update node status in DynamoDB
            if node_name:
                update_node_status(node_table, node_name, 'REBOOTING')

            # Send reboot command
            if not reboot_instance(instance_id):
                return False

        return True
    except Exception as e:
        logger.error(f"Error handling exit code 1: {str(e)}")
        return False

# ----- Main Lambda Handler -----

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for monitoring DCGM task completion events.
    This function is triggered when a DCGM task completes and takes appropriate actions
    based on the exit code and job configuration.

    Args:
        event: The Lambda event
        context: The Lambda context

    Returns:
        dict: Response with status code and message
    """
    logger.info('Event received: %s', json.dumps(event))

    # Get environment variables
    task_table_name = os.environ.get('TASK_TABLE_NAME')
    job_table_name = os.environ.get('JOB_TABLE_NAME')
    node_table_name = os.environ.get('NODE_TABLE_NAME')

    # Validate environment variables
    missing = []
    if not task_table_name: missing.append('TASK_TABLE_NAME')
    if not job_table_name: missing.append('JOB_TABLE_NAME')
    if not node_table_name: missing.append('NODE_TABLE_NAME')

    if missing:
        error_msg = f"Missing required environment variables: {', '.join(missing)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        }

    # Initialize DynamoDB tables
    task_table = dynamodb.Table(task_table_name)
    job_table = dynamodb.Table(job_table_name)
    node_table = dynamodb.Table(node_table_name)

    # Validate event
    if not is_valid_ecs_event(event):
        return {
            'statusCode': 400,
            'body': 'Function only supports input from ECS events'
        }

    if not is_task_state_change(event):
        return {
            'statusCode': 200,
            'body': 'Not a task state change event'
        }

    detail = event["detail"]

    if not is_stopped_task(detail):
        return {
            'statusCode': 200,
            'body': f"Task status is {detail.get('lastStatus')}, no action needed"
        }

    # Extract task and cluster information
    task_arn = detail['taskArn']
    task_id = task_arn.split('/')[-1]
    cluster_arn = detail['clusterArn']
    cluster_name = cluster_arn.split('/')[-1]

    logger.info(f"Processing stopped task {task_id} in cluster {cluster_name}")

    # Get task details
    try:
        task_detail = ecs_client.describe_tasks(
            cluster=cluster_name,
            tasks=[task_id]
        )["tasks"][0]
    except Exception as e:
        logger.error(f"Error getting task details: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error getting task details: {str(e)}"
        }

    # Get the job ID associated with this task
    job_id = get_job_id_from_task(task_id, cluster_name)
    if not job_id:
        return {
            'statusCode': 200,
            'body': 'No job_id tag found on task'
        }

    # Get job and task information from DynamoDB
    job_record, task_records = get_job_and_tasks(job_table, task_table, job_id)
    if not job_record:
        return {
            'statusCode': 200,
            'body': f"No job record found for job_id {job_id}"
        }

    # Process containers based on exit code
    containers = task_detail.get('containers')
    if not containers:
        return {
            'statusCode': 500,
            'body': 'No containers found in task detail'
        }

    # Process each container's exit code
    for container in containers:
        exit_code = container.get('exitCode')
        logger.info(f"Container {container.get('name')} exit code: {exit_code}")

        if exit_code == 0:
            # Task exited with code 0, update job status to failed
            if not handle_exit_code_0(job_table, task_table, node_table, job_record, task_records, cluster_arn):
                return {
                    'statusCode': 500,
                    'body': 'Error handling exit code 0'
                }

        elif exit_code == 1:
            # Task exited with code 1, check if reboot needed
            if not handle_exit_code_1(job_table, task_table, node_table, detail, cluster_arn, job_record, task_records):
                return {
                    'statusCode': 500,
                    'body': 'Error handling exit code 1'
                }

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
