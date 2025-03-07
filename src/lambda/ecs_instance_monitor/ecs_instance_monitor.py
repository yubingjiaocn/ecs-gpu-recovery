import json
import logging

from config import Config
from utils import validate_ecs_container_instance_event
from instance_processor import process_active_instance

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda handler for ECS Container Instance State Change events.
    Monitors instances being restarted and handles related jobs and tasks.

    Args:
        event (dict): Lambda event from EventBridge
        context (LambdaContext): Lambda context

    Returns:
        dict: Response
    """
    logger.info('[LAMBDA_START] ECS Instance Monitor invoked')
    logger.info(f'[EVENT_RECEIVED] Event: {json.dumps(event)}')

    # Initialize configuration
    config = Config()

    # Validate event
    if not validate_ecs_container_instance_event(event):
        logger.error('[VALIDATION_FAILED] Invalid event format')
        return {
            'statusCode': 400,
            'body': 'Invalid event format'
        }

    detail = event["detail"]

    # Extract instance ID
    instance_id = detail.get('containerInstanceArn', '')
    if not instance_id:
        logger.error("[EVENT_ERROR] No instance ID in event")
        return {
            'statusCode': 400,
            'body': 'No instance ID in event'
        }

    instance_id = instance_id.split('/')[-1]
    logger.info(f"[INSTANCE_ID] Extracted instance ID: {instance_id}")

    # Check if this is an instance starting up (ACTIVE status)
    if detail.get('status') != 'ACTIVE':
        logger.info(f"[INSTANCE_STATUS] Container instance status is {detail.get('status')}, no action needed")
        return {
            'statusCode': 200,
            'body': f"Container instance status is {detail.get('status')}, no action needed"
        }

    # Process the active instance
    result = process_active_instance(instance_id, detail, config)

    logger.info('[LAMBDA_COMPLETE] ECS Instance Monitor completed')
    return result
