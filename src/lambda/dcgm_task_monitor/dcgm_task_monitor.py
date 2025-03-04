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

def validate_ecs_event(event):
    """
    Validates that the event is an ECS event
    """
    if event.get("source") != "aws.ecs":
        logger.error("Function only supports input from events with a source type of: aws.ecs")
        return False
    return True

def validate_task_state_change(event):
    """
    Validates that the event is a task state change event
    """
    if event["detail-type"] != "ECS Task State Change":
        logger.info("Not a task state change event, ignoring")
        return False
    return True

def validate_stopped_task(detail):
    """
    Validates that the task has stopped
    """
    if detail.get('lastStatus') != 'STOPPED':
        logger.info(f"Task status is {detail.get('lastStatus')}, no action needed")
        return False
    return True

def get_job_id_from_task(task_id, cluster_name):
    """
    Gets the job ID associated with a task from its tags

    Returns:
        str: The job ID if found, None otherwise
    """
    describe_task_response = ecs_client.describe_tasks(
        cluster=cluster_name,
        tasks=[task_id],
        include=['TAGS']
    )

    job_id = None
    if 'tasks' in describe_task_response and len(describe_task_response['tasks']) > 0:
        task = describe_task_response['tasks'][0]
        tags = task.get('tags', [])
        for tag in tags:
            logger.info(f"Tag: {tag['key']}, Value: {tag['value']}")
            if tag['key'] == 'job_id':
                job_id = tag['value']
                break

    return job_id

def get_job_data(table, job_id):
    """
    Gets job information from DynamoDB

    Returns:
        dict: The job data if found, None otherwise
    """
    job_dynamodb_data = table.scan(
        FilterExpression=Attr('job_id').eq(job_id)
    )

    if not job_dynamodb_data or 'Items' not in job_dynamodb_data or not job_dynamodb_data['Items']:
        logger.warning(f"No job data found for job_id {job_id}, exiting")
        return None

    return job_dynamodb_data

def handle_exit_code_0(table, job_dynamodb_data):
    """
    Handles a task with exit code 0 (successful DCGM run but job failed)
    """
    logger.info("DCGM task exit code 0, updating job status to FAILED")
    job_id_ranks = [item['job_id_rank'] for item in job_dynamodb_data['Items']]

    for job_id_rank in job_id_ranks:
        response = table.update_item(
            Key={'job_id_rank': job_id_rank},
            UpdateExpression='SET job_status = :val',
            ExpressionAttributeValues={
                ':val': 'FAILED'
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"Updated job {job_id_rank} status to FAILED: {response}")

def get_instance_id(cluster_arn, container_instance_arn):
    """
    Gets the EC2 instance ID from the container instance ARN

    Returns:
        str: The instance ID if found, None otherwise
    """
    instance_id = ecs_client.describe_container_instances(
        cluster=cluster_arn,
        containerInstances=[container_instance_arn]
    )['containerInstances'][0]['ec2InstanceId']
    return instance_id

def reboot_instance(instance_id):
    """
    Reboots an instance using SSM

    Returns:
        dict: The SSM response, None if an error occurred
    """
    try:
        logger.info(f"Rebooting instance {instance_id}")
        ssm_response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': ['reboot']}
        )
        logger.info(f"Reboot command execution response: {ssm_response}")
        return ssm_response
    except Exception as e:
        logger.error(f"Error executing reboot command: {str(e)}")
        return None

def update_container_instance(container_instance_arn):
    """
    Updates container instance status in DynamoDB

    Returns:
        dict: The DynamoDB response, None if no table name provided
    """
    container_inst_id = container_instance_arn.split('/')[-1]
    container_inst_table_name = os.environ.get('CONTAINER_INSTANCE_TABLE_NAME')

    if not container_inst_table_name:
        return None

    container_inst_table = dynamodb.Table(container_inst_table_name)
    response = container_inst_table.update_item(
        Key={'container_inst_id': container_inst_id},
        UpdateExpression='SET #s = :val',
        ExpressionAttributeNames={
            '#s': 'status'
        },
        ExpressionAttributeValues={
            ':val': 'REBOOTED'
        },
        ReturnValues="UPDATED_NEW"
    )
    logger.info(f"Updated container instance status: {response}")
    return response

def handle_exit_code_1(detail, cluster_arn, job_dynamodb_data):
    """
    Handles a task with exit code 1 (DCGM detected an issue)

    Returns:
        tuple: (success, error_message)
    """
    # Check retry to determine if reboot is needed
    retry = job_dynamodb_data['Items'][0].get('retry')
    logger.info(f"DCGM task exit code 1, retry: {retry}")

    if retry and int(retry) == 0:
        # Reboot the instance using SSM
        logger.info("retry is 0, initiating instance reboot")

        # Get instance ID from container instance
        instance_id = get_instance_id(cluster_arn, detail['containerInstanceArn'])
        if not instance_id:
            return False, "Failed to get container instance details"

        ecs_client.put_attributes(
            cluster=cluster_arn,
            attributes=[
                {
                    'name': 'status',
                    'value': 'REBOOTING'
                }
            ],
            targetId=detail['containerInstanceArn']
        )

        # Send reboot command
        ssm_response = reboot_instance(instance_id)
        if not ssm_response:
            return False, "Error executing reboot command"

        # Update container instance status
        update_container_instance(detail['containerInstanceArn'])

    return True, None

def lambda_handler(event, context):
    """
    Lambda handler for monitoring DCGM task completion events
    This function is triggered when a DCGM task completes and takes appropriate actions
    based on the exit code and job configuration
    """
    logger.info('Event received: %s', json.dumps(event))

    # Get environment variables
    training_job_table_name = os.environ.get('TRAINING_JOB_TABLE_NAME')
    table = dynamodb.Table(training_job_table_name)

    # Validate event
    if not validate_ecs_event(event):
        return {
            'statusCode': 400,
            'body': 'Function only supports input from ECS events'
        }

    if not validate_task_state_change(event):
        return {
            'statusCode': 200,
            'body': 'Not a task state change event'
        }

    detail = event["detail"]

    if not validate_stopped_task(detail):
        return {
            'statusCode': 200,
            'body': f"Task status is {detail.get('lastStatus')}, no action needed"
        }

    task_arn = detail['taskArn']
    task_id = task_arn.split('/')[-1]
    cluster_arn = detail['clusterArn']
    cluster_name = cluster_arn.split('/')[-1]

    # Get task detail using describeTasks
    task_detail = ecs_client.describe_tasks(
        cluster=cluster_name,
        tasks=[task_id]
    )["tasks"][0]

    logger.info(f"Processing stopped task {task_id} in cluster {cluster_name}")

    # Get the job ID associated with this task
    job_id = get_job_id_from_task(task_id, cluster_name)
    if not job_id:
        return {
            'statusCode': 200,
            'body': 'No job_id tag found on task'
        }

    # Get job information from DynamoDB
    job_dynamodb_data = get_job_data(table, job_id)
    if not job_dynamodb_data:
        return {
            'statusCode': 200,
            'body': f"No job data found for job_id {job_id}"
        }

    # Process containers based on exit code
    exit_code = None
    containers = task_detail.get('containers', None)
    if containers is None:
        return {
            'statusCode': 500,
            'body': 'No containers found in task detail'
        }
    else:
        for container in containers:
            exit_code = container.get('exitCode')
            logger.info(f"Container {container.get('name')} exit code: {exit_code}")

            if exit_code == 0:
                # Task exited with code 0, update job status to failed
                handle_exit_code_0(table, job_dynamodb_data)

            elif exit_code == 1:
                # Task exited with code 1, check if reboot needed
                success, error_message = handle_exit_code_1(detail, cluster_arn, job_dynamodb_data)
                if not success:
                    return {
                        'statusCode': 500,
                        'body': error_message
                    }

        return {
            'statusCode': 200,
            'body': 'Processing complete'
        }
