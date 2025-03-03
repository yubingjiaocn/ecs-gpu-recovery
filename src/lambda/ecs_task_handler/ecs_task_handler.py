import json
import boto3
import os
import logging
from boto3.dynamodb.conditions import Attr

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ecs_client = boto3.client('ecs')
ssm_client = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')

def get_environment_variables():
    """
    Get environment variables needed for the Lambda function.
    """
    return {
        'training_job_table_name': os.environ.get('TRAINING_JOB_TABLE_NAME'),
        'ecs_cluster_name': os.environ.get('ECS_CLUSTER_NAME'),
        'dcgm_health_check_task': os.environ.get('DCGM_HEALTH_CHECK_TASK')
    }

def parse_event_message(event_dict, event_attributes):
    """
    Parse event message and extract relevant attributes.

    Args:
        event_dict (dict): Event dictionary
        event_attributes (list): List of attributes to extract

    Returns:
        dict: Dictionary containing extracted attributes
    """
    logger.info("Parsing event")
    event_detail = {}
    for attribute in event_attributes:
        if event_dict.get(attribute):
            event_detail[attribute] = event_dict[attribute]
    return event_detail

def get_job_by_task_id(task_id, table):
    """
    Get job information by task ID.

    Args:
        task_id (str): ECS task ID
        table (DynamoDB.Table): DynamoDB table object

    Returns:
        tuple: (job_id, job_records) if found, (None, None) if not found
    """
    try:
        response = table.scan(
            FilterExpression=Attr('ecs_task_id').eq(task_id)
        )

        if response.get('Items'):
            job_id = response['Items'][0]['job_id']
            logger.info(f"Found job ID: {job_id} for task ID: {task_id}")

            # Get all records for this job
            job_records = table.scan(
                FilterExpression=Attr('job_id').eq(job_id)
            )

            return job_id, job_records.get('Items', [])
        else:
            logger.warning(f"No job found for task ID: {task_id}")
            return None, None
    except Exception as e:
        logger.error(f"Error getting job by task ID: {str(e)}")
        return None, None

def update_job_status(table, job_records, status):
    """
    Update status for all records associated with a job.

    Args:
        table (DynamoDB.Table): DynamoDB table object
        job_records (list): List of job records
        status (str): Status to update to
    """
    try:
        for record in job_records:
            job_id_rank = record.get('job_id_rank')
            if job_id_rank:
                table.update_item(
                    Key={'job_id_rank': job_id_rank},
                    UpdateExpression='SET status = :status',
                    ExpressionAttributeValues={':status': status}
                )
                logger.info(f"Updated job_id_rank {job_id_rank} status to {status}")
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")

def stop_tasks_for_job(cluster_name, task_ids):
    """
    Stop all tasks associated with a job.

    Args:
        cluster_name (str): ECS cluster name
        task_ids (list): List of task IDs to stop
    """
    for task_id in task_ids:
        try:
            logger.info(f"Stopping task {task_id}")
            ecs_client.stop_task(
                cluster=cluster_name,
                task=task_id
            )
        except Exception as e:
            logger.error(f"Error stopping task {task_id}: {str(e)}")

def run_dcgm_health_check(cluster_arn, container_instance_arn, job_id, dcgm_task_def):
    """
    Run DCGM health check task on a container instance.

    Args:
        cluster_arn (str): ECS cluster ARN
        container_instance_arn (str): Container instance ARN
        job_id (str): Job ID
        dcgm_task_def (str): DCGM health check task definition

    Returns:
        dict: ECS run_task response
    """
    try:
        # Run DCGM health check task
        logger.info(f"Running DCGM health check task on {container_instance_arn}")
        dcgm_response = ecs_client.start_task(
            cluster=cluster_arn,
            taskDefinition=dcgm_task_def,
            containerInstances=[container_instance_arn],
            tags=[{
                'key': 'job_id',
                'value': job_id
            }]
        )
        logger.info(f"DCGM health check task response: {dcgm_response}")
        return dcgm_response
    except Exception as e:
        logger.error(f"Error running DCGM health check task: {str(e)}")
        return None

def handle_user_stopped_task(task_id, table):
    """
    Handle task stopped by user.
    Based on flowchart, update the job status to Other.

    Args:
        task_id (str): ECS task ID
        table (DynamoDB.Table): DynamoDB table object
    """
    logger.info("Task was stopped by user")

    job_id, job_records = get_job_by_task_id(task_id, table)
    if job_id and job_records:
        update_job_status(table, job_records, 'Other')

def handle_task_exit_code_1(task_id, cluster_arn, cluster_name, table, env_vars):
    """
    Handle task with exit code 1.
    Based on flowchart, stop all tasks for this job and run DCGM health check.

    Args:
        task_id (str): ECS task ID
        cluster_arn (str): ECS cluster ARN
        cluster_name (str): ECS cluster name
        table (DynamoDB.Table): DynamoDB table object
        env_vars (dict): Environment variables
    """
    logger.info("Task exited with code 1")

    job_id, job_records = get_job_by_task_id(task_id, table)
    if job_id and job_records:
        # Get all task IDs for this job
        task_ids = []
        for record in job_records:
            task_id = record.get('ecs_task_id')
            if task_id:
                task_ids.append(task_id)

        # Stop all tasks for this job
        stop_tasks_for_job(cluster_name, task_ids)

        # Query task details to get container instance attributes
        tasks_response = ecs_client.describe_tasks(
            cluster=cluster_name,
            tasks=[task_ids[0] if task_ids else task_id]  # Use the first task ID or current task ID
        )

        container_instance_arns = []
        for task in tasks_response.get('tasks', []):
            if task.get('containerInstanceArn') and task['containerInstanceArn'] not in container_instance_arns:
                container_instance_arns.append(task['containerInstanceArn'])

        # Run GPU DCGM check task on each container instance
        for container_instance_arn in container_instance_arns:
            run_dcgm_health_check(
                cluster_arn,
                container_instance_arn,
                job_id,
                env_vars['dcgm_health_check_task']
            )

        # Update status to 'stop' for all records with this job ID
        update_job_status(table, job_records, 'stop')

def handle_task_exit_code_0(task_id, table):
    """
    Handle task with exit code 0.
    Based on flowchart, check current status and update if needed.

    Args:
        task_id (str): ECS task ID
        table (DynamoDB.Table): DynamoDB table object
    """
    logger.info("Task exited with code 0")

    job_id, job_records = get_job_by_task_id(task_id, table)
    if job_id and job_records:
        # Check current status
        current_status = job_records[0].get('status')
        if current_status == 'complete':
            logger.info("Job status is already complete, exiting")
            return

        if current_status == 'inprogress':
            # Update status to 'complete'
            update_job_status(table, job_records, 'complete')

def handle_task_state_change(detail, env_vars):
    """
    Handle ECS Task State Change event according to flowchart

    Args:
        detail (dict): Event detail
        env_vars (dict): Environment variables
    """
    # Check if task is STOPPED
    if detail.get('lastStatus') != 'STOPPED':
        logger.info(f"Task status is {detail.get('lastStatus')}, no action needed")
        return

    task_id = detail['taskArn'].split('/')[2]
    cluster_arn = detail['clusterArn']
    cluster_name = cluster_arn.split('/')[1]

    logger.info(f"Processing stopped task {task_id} in cluster {cluster_name}")

    # Get task detail using describeTasks
    task_detail = ecs_client.describe_tasks(
        cluster=cluster_name,
        tasks=[task_id]
    )

    # Get DynamoDB table
    table = dynamodb.Table(env_vars['training_job_table_name'])

    stop_code = task_detail.get('stopCode')

    # Check if task was stopped by user (based on stopCode or other attributes)
    if stop_code.startswith('UserInitiated'):
        handle_user_stopped_task(task_id, table)
        return
    else:
        exit_code = None
        containers = task_detail.get('containers', [])
        for container in containers:
            if container.get('exitCode') is not None:
                exit_code = container.get('exitCode')
                break

        logger.info(f"Task exit code: {exit_code}")

        if exit_code == 1:
            handle_task_exit_code_1(task_id, cluster_arn, cluster_name, table, env_vars)
        elif exit_code == 0:
            handle_task_exit_code_0(task_id, table)
        return
    return

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

    # Get environment variables
    env_vars = get_environment_variables()

    # Verify this is an ECS event from EventBridge
    if event.get("source") != "aws.ecs":
        logger.error("Function only supports input from events with a source type of: aws.ecs")
        return {
            'statusCode': 400,
            'body': 'Function only supports input from events with a source type of: aws.ecs'
        }

    # Handle ECS Task State Change
    if event["detail-type"] == "ECS Task State Change":
        handle_task_state_change(event["detail"], env_vars)

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
