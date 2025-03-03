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

def get_environment_variables():
    """
    Get environment variables needed for the Lambda function.
    """
    return {
        'training_job_table_name': os.environ.get('TRAINING_JOB_TABLE_NAME'),
        'sns_topic_arn': os.environ.get('SNS_TOPIC_ARN')
    }

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
    Query jobs associated with an EC2 instance ID.

    Args:
        instance_id (str): EC2 instance ID
        table_name (str): DynamoDB table name

    Returns:
        list: List of job records associated with the instance
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.scan(
            FilterExpression=Attr('ec2_instance_id').eq(instance_id)
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

def run_training_task(cluster_name, task_definition, container_instance_arn, job_id, task_info):
    """
    Re-execute a training task on a container instance.

    Args:
        cluster_name (str): ECS cluster name
        task_definition (str): Task definition ARN or name
        container_instance_arn (str): Container instance ARN
        job_id (str): Job ID
        task_info (dict): Task information

    Returns:
        dict: Response from run_task API
    """
    try:
        # Prepare tags for the task
        tags = [
            {
                'key': 'job_id',
                'value': job_id
            }
        ]

        # Add any additional tags from the original task
        if task_info.get('tags'):
            for tag in task_info.get('tags', []):
                if tag['key'] != 'job_id':  # Avoid duplicate job_id tag
                    tags.append(tag)

        # Run the task on the specified container instance
        response = ecs_client.run_task(
            cluster=cluster_name,
            taskDefinition=task_definition,
            count=1,
            startedBy='ecs-instance-monitor-lambda',
            containerInstances=[container_instance_arn],
            tags=tags
        )

        logger.info(f"Started training task: {response}")
        return response
    except Exception as e:
        logger.error(f"Error running training task: {str(e)}")
        return None

def update_job_run_time(table_name, job_id_rank, run_time):
    """
    Update the run_time of a job in DynamoDB.

    Args:
        table_name (str): DynamoDB table name
        job_id_rank (str): Job ID rank (primary key)
        run_time (int): New run_time value

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        table = dynamodb.Table(table_name)
        table.update_item(
            Key={'job_id_rank': job_id_rank},
            UpdateExpression='SET run_time = :val',
            ExpressionAttributeValues={':val': str(run_time)}
        )
        logger.info(f"Updated job {job_id_rank} run_time to {run_time}")
        return True
    except Exception as e:
        logger.error(f"Error updating job run_time: {str(e)}")
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
    env_vars = get_environment_variables()

    # Validate event
    if not validate_ecs_container_instance_event(event):
        return {
            'statusCode': 400,
            'body': 'Invalid event format'
        }

    detail = event["detail"]
    instance_id = detail.get('ec2InstanceId')

    if not instance_id:
        logger.error("No EC2 instance ID in event")
        return {
            'statusCode': 400,
            'body': 'No EC2 instance ID in event'
        }

    # Check if this is an instance starting up (ACTIVE status)
    if detail.get('status') != 'ACTIVE':
        logger.info(f"Container instance status is {detail.get('status')}, no action needed")
        return {
            'statusCode': 200,
            'body': f"Container instance status is {detail.get('status')}, no action needed"
        }

    logger.info(f"Processing restarted instance {instance_id}")

    # Query training jobs associated with this instance
    jobs = get_jobs_by_instance_id(instance_id, env_vars['training_job_table_name'])

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
        run_time = job.get('run_time', '0')

        logger.info(f"Processing job: {job_id}, status: {job_status}, run_time: {run_time}")

        # Skip if job status is 'stop'
        if job_status == 'stop':
            logger.info(f"Job {job_id} status is 'stop', skipping")
            continue

        # Check run_time
        if run_time == '1':
            # Re-execute training task and update run_time to 2
            logger.info(f"Job {job_id} run_time is 1, re-executing training task")

            # Get task definition and container instance ARN
            task_definition = job.get('task_definition')
            container_instance_arn = detail.get('containerInstanceArn')
            cluster_name = detail.get('clusterArn').split('/')[-1]

            # Get more information about the existing task
            task_info = {
                'job_id': job_id,
                'tags': job.get('tags', [])
            }

            # Run the training task
            response = run_training_task(
                cluster_name,
                task_definition,
                container_instance_arn,
                job_id,
                task_info
            )

            if response:
                # Update run_time to 2
                update_job_run_time(env_vars['training_job_table_name'], job_id_rank, 2)
                processed_jobs += 1
        else:
            # Update job status to 'fail' and notify technical staff
            logger.info(f"Job {job_id} run_time is not 1, updating status to 'fail'")
            update_job_status(env_vars['training_job_table_name'], job_id_rank, 'fail')

            # Send notification to technical staff
            if env_vars.get('sns_topic_arn'):
                subject = f"Job {job_id} failed after instance restart"
                message = (f"Job {job_id} on instance {instance_id} failed after restart. "
                          f"The job has been marked as failed. Please investigate.")
                send_notification(env_vars['sns_topic_arn'], subject, message)

            processed_jobs += 1

    return {
        'statusCode': 200,
        'body': f"Processed {processed_jobs} jobs"
    }
