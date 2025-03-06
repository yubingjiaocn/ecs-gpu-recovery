import json
import boto3
import os
import logging
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

def get_job_data(table, job_id: str) -> Optional[Dict[str, Any]]:
    """
    Gets job information from DynamoDB.

    Args:
        table: The DynamoDB table
        job_id: The job ID

    Returns:
        dict: The job data if found, None otherwise
    """
    try:
        job_dynamodb_data = table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        )

        if not job_dynamodb_data or 'Items' not in job_dynamodb_data or not job_dynamodb_data['Items']:
            logger.warning(f"No job data found for job_id {job_id}")
            return None

        return job_dynamodb_data
    except Exception as e:
        logger.error(f"Error getting job data: {str(e)}")
        return None

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
                    'value': status
                }
            ],
            targetId=container_instance_arn
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

def update_container_instance_in_dynamodb(container_instance_arn: str, status: str = 'REBOOTED') -> bool:
    """
    Updates container instance status in DynamoDB.

    Args:
        container_instance_arn: The container instance ARN
        status: The new status

    Returns:
        bool: True if successful, False otherwise
    """
    container_inst_table_name = os.environ.get('CONTAINER_INSTANCE_TABLE_NAME')
    if not container_inst_table_name:
        logger.warning("CONTAINER_INSTANCE_TABLE_NAME environment variable not set")
        return False

    try:
        container_inst_id = container_instance_arn.split('/')[-1]
        container_inst_table = dynamodb.Table(container_inst_table_name)

        container_inst_table.update_item(
            Key={'container_inst_id': container_inst_id},
            UpdateExpression='SET #s = :val',
            ExpressionAttributeNames={
                '#s': 'status'
            },
            ExpressionAttributeValues={
                ':val': status
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"Updated container instance {container_inst_id} status to {status} in DynamoDB")
        return True
    except Exception as e:
        logger.error(f"Error updating container instance in DynamoDB: {str(e)}")
        return False

# ----- Job Status Management Functions -----

def update_job_status(table, job_id_ranks: List[str], status: str) -> bool:
    """
    Updates job status in DynamoDB for multiple job_id_rank entries.

    Args:
        table: The DynamoDB table
        job_id_ranks: List of job_id_rank values
        status: The new status

    Returns:
        bool: True if all updates were successful, False otherwise
    """
    success = True
    for job_id_rank in job_id_ranks:
        try:
            response = table.update_item(
                Key={'job_id_rank': job_id_rank},
                UpdateExpression='SET job_status = :val',
                ExpressionAttributeValues={
                    ':val': status
                },
                ReturnValues="UPDATED_NEW"
            )
            logger.info(f"Updated job {job_id_rank} status to {status}")
        except Exception as e:
            logger.error(f"Error updating job {job_id_rank} status: {str(e)}")
            success = False

    return success

# ----- Exit Code Handlers -----

def handle_exit_code_0(table, job_data: Dict[str, Any], cluster_arn: str) -> bool:
    """
    Handles a task with exit code 0 (successful DCGM run but job failed).
    Updates job status to FAILED and releases container instances.

    Args:
        table: The DynamoDB table
        job_data: The job data from DynamoDB
        cluster_arn: The cluster ARN

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("DCGM task exit code 0, updating job status to FAILED")

    try:
        # Extract job_id_ranks and container instances
        job_id_ranks = [item['job_id_rank'] for item in job_data['Items']]
        container_instances = {}

        # Identify failed and other instances
        for item in job_data['Items']:
            container_inst_arn = item.get('containerInstanceArn')
            if container_inst_arn:
                container_instances[container_inst_arn] = True

        # Release related instances to AVAILABLE
        for container_inst_arn in container_instances:
            update_container_instance_status(cluster_arn, container_inst_arn, 'AVAILABLE')

        # Update job status in DynamoDB
        update_job_status(table, job_id_ranks, 'FAILED')

        return True
    except Exception as e:
        logger.error(f"Error handling exit code 0: {str(e)}")
        return False

def handle_exit_code_1(detail: Dict[str, Any], cluster_arn: str, job_data: Dict[str, Any]) -> bool:
    """
    Handles a task with exit code 1 (DCGM detected an issue).
    May reboot the instance if retry count is 0.

    Args:
        detail: The detail section of the event
        cluster_arn: The cluster ARN
        job_data: The job data from DynamoDB

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check retry to determine if reboot is needed
        retry = job_data['Items'][0].get('retry')
        logger.info(f"DCGM task exit code 1, retry: {retry}")

        if retry and int(retry) == 0:
            logger.info("retry is 0, initiating instance reboot")
            container_instance_arn = detail['containerInstanceArn']

            # Get instance ID from container instance
            instance_id = get_instance_id(cluster_arn, container_instance_arn)
            if not instance_id:
                logger.error("Failed to get container instance details")
                return False

            # Mark instance as REBOOTING
            update_container_instance_status(cluster_arn, container_instance_arn, 'REBOOTING')

            # Send reboot command
            if not reboot_instance(instance_id):
                return False

            # Update container instance status in DynamoDB
            update_container_instance_in_dynamodb(container_instance_arn)

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
    training_job_table_name = os.environ.get('TRAINING_JOB_TABLE_NAME')
    if not training_job_table_name:
        logger.error("TRAINING_JOB_TABLE_NAME environment variable not set")
        return {
            'statusCode': 500,
            'body': 'TRAINING_JOB_TABLE_NAME environment variable not set'
        }

    table = dynamodb.Table(training_job_table_name)

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

    # Get job information from DynamoDB
    job_data = get_job_data(table, job_id)
    if not job_data:
        return {
            'statusCode': 200,
            'body': f"No job data found for job_id {job_id}"
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
            if not handle_exit_code_0(table, job_data, cluster_arn):
                return {
                    'statusCode': 500,
                    'body': 'Error handling exit code 0'
                }

        elif exit_code == 1:
            # Task exited with code 1, check if reboot needed
            if not handle_exit_code_1(detail, cluster_arn, job_data):
                return {
                    'statusCode': 500,
                    'body': 'Error handling exit code 1'
                }

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
