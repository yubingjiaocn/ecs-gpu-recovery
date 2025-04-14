import logging
from dynamodb_service import DynamoDBService
from ecs_service import ECSService
from notification_service import NotificationService
from job_processor import JobProcessor

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

    # Check if the instance has attributes and get the current status
    container_instance_arn = detail.get('containerInstanceArn')
    if container_instance_arn:
        # Get instance attributes to check current status
        try:
            attributes = ecs_service.get_container_instance_attributes(container_instance_arn)
            if attributes:
                current_status = attributes.get('status')

                # If status is REBOOTING, change it to AVAILABLE
                if current_status == 'REBOOTING':
                    logger.info(f"[INSTANCE_STATE_CHANGE] Changing instance {container_instance_arn} status from REBOOTING to AVAILABLE")
                    ecs_service.set_instance_status(container_instance_arn, 'AVAILABLE')
                else:
                    logger.info(f"[INSTANCE_STATE_INFO] Instance {container_instance_arn} status is {current_status}, no change needed")
        except Exception as e:
            logger.error(f"[INSTANCE_STATE_ERROR] Error checking/updating instance status: {str(e)}")

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
