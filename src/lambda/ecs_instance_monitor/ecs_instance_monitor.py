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
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

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

def get_jobs_by_instance_id(instance_id, table_name):
    """
    Query jobs associated with a container instance ID.

    Args:
        instance_id (str): Container instance ID
        table_name (str): DynamoDB table name

    Returns:
        list: List of job records associated with the instance
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.scan(
            FilterExpression=Attr('container_inst_id').eq(instance_id)
        )

        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error querying jobs by instance ID: {str(e)}")
        return []

def update_job_status(table_name, job_id_rank, status):
    """
    Update the status of a job in DynamoDB.

    Args:
        table_name (str): DynamoDB table name
        job_id_rank (str): Job ID rank (primary key)
        status (str): New status value

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        table = dynamodb.Table(table_name)
        table.update_item(
            Key={'job_id_rank': job_id_rank},
            UpdateExpression='SET job_status = :val',
            ExpressionAttributeValues={':val': status}
        )
        logger.info(f"Updated job {job_id_rank} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")
        return False

def run_training_task(cluster_name, task_info):
    """
    Re-execute a training task on a container instance.

    Args:
        cluster_name (str): ECS cluster name
        task_info (dict): Task information from original task definition

    Returns:
        dict: Response from run_task API
    """
    try:
        task_definition = task_info['taskDefinitionArn']
        container_instance_arn = task_info['containerInstanceArn']
        tags = task_info.get('tags', [])
        overrides = task_info.get('overrides', {})

        # Run the task on the specified container instance
        response = ecs_client.start_task(
            cluster=cluster_name,
            taskDefinition=task_definition,
            startedBy='ecs-instance-monitor-lambda',
            containerInstances=[container_instance_arn],
            tags=tags,
            overrides=overrides
        )

        logger.info(f"Started training task: {response}")

        return response
    except Exception as e:
        logger.error(f"Error running training task: {str(e)}")
        return None

def update_job_retry(table_name, job_id_rank, retry, new_task_id):
    """
    Update the retry of a job in DynamoDB.

    Args:
        table_name (str): DynamoDB table name
        job_id_rank (str): Job ID rank (primary key)
        retry (int): New retry value
        new_task_id (str): New ECS Task ID

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        table = dynamodb.Table(table_name)
        table.update_item(
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
    except Exception as e:
        logger.error(f"Error updating job retry: {str(e)}")
        return False

def send_notification(sns_topic_arn, subject, message):
    """
    Send an SNS notification.

    Args:
        sns_topic_arn (str): SNS topic ARN
        subject (str): Notification subject
        message (str): Notification message

    Returns:
        dict: Response from SNS publish API
    """
    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"Sent notification: {subject}")
        return response
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")
        return None

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

    # Get environment variables
    training_job_table_name = os.environ.get('TRAINING_JOB_TABLE_NAME')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')

    # Validate event
    if not validate_ecs_container_instance_event(event):
        return {
            'statusCode': 400,
            'body': 'Invalid event format'
        }

    detail = event["detail"]
    instance_id = detail.get('containerInstanceArn')
    instance_id = instance_id.split('/')[-1]

    if not instance_id:
        logger.error("No instance ID in event")
        return {
            'statusCode': 400,
            'body': 'No instance ID in event'
        }

    # Check if this is an instance starting up (ACTIVE status)
    if detail.get('status') != 'ACTIVE':
        logger.info(f"Container instance status is {detail.get('status')}, no action needed")
        return {
            'statusCode': 200,
            'body': f"Container instance status is {detail.get('status')}, no action needed"
        }

    logger.info(f"Processing restarted instance {instance_id}")

    ecs_client.put_attributes(
        cluster=ecs_cluster_name,
        attributes=[
            {
                'name': 'status',
                'value': 'AVAILABLE'
            }
        ],
        targetId=detail['containerInstanceArn']
    )

    # Query training jobs associated with this instance
    jobs = get_jobs_by_instance_id(instance_id, training_job_table_name)

    if not jobs:
        logger.info(f"No jobs found for instance {instance_id}")
        return {
            'statusCode': 200,
            'body': f"No jobs found for instance {instance_id}"
        }

    processed_jobs = 0

    # Process each job
    for job in jobs:
        job_id = job.get('job_id')
        job_id_rank = job.get('job_id_rank')
        job_status = job.get('job_status')
        retry = job.get('retry', 0)

        logger.info(f"Processing job: {job_id}, status: {job_status}, retry: {retry}")

        # Skip if job status is 'FAILED'
        if job_status == 'FAILED':
            logger.info(f"Job {job_id} status is 'FAILED', skipping")
            continue

        # Check retry
        if retry == 0:
            # Re-execute training task and update retry to 1
            logger.info(f"Job {job_id} retry is 0, re-executing training task")

            original_task_id = job.get('ecs_task_id')
            original_task_info = ecs_client.describe_tasks(
                cluster=detail.get('clusterArn').split('/')[-1],
                tasks=[original_task_id]
            )['tasks'][0]

            # Run the training task
            response = run_training_task(
                ecs_cluster_name,
                original_task_info
            )

            if response:
                # Update retry to 1
                new_task_id = response['tasks'][0]["taskArn"].split('/')[-1]
                update_job_retry(training_job_table_name, job_id_rank, 1, new_task_id)
                processed_jobs += 1
        else:
            # Update job status to 'fail' and notify technical staff
            logger.info(f"Job {job_id} retry is not 1, updating status to 'fail'")
            update_job_status(training_job_table_name, job_id_rank, 'fail')

            # Send notification to technical staff
            subject = f"Job {job_id} failed after instance restart"
            message = (f"Job {job_id} on instance {instance_id} failed after restart. "
                        f"The job has been marked as failed. Please investigate.")
            send_notification(sns_topic_arn, subject, message)

            processed_jobs += 1

    return {
        'statusCode': 200,
        'body': f"Processed {processed_jobs} jobs"
    }
