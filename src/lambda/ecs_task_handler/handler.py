import json
from common import logger
from config import Config
from dynamodb_service import DynamoDBService
from ecs_service import ECSService
from task_processor import TaskProcessor
from utils import validate_ecs_task_event

def lambda_handler(event, context):
    """
    Lambda handler for ECS task events directly from EventBridge

    Args:
        event (dict): Lambda event from EventBridge
        context (LambdaContext): Lambda context

    Returns:
        dict: Response
    """
    logger.info('[LAMBDA_START] ECS Task Handler invoked')
    logger.info(f'[EVENT_RECEIVED] Event: {json.dumps(event)}')

    # Initialize configuration
    config = Config()

    # Validate event
    if not validate_ecs_task_event(event):
        logger.error('[VALIDATION_FAILED] Invalid event source')
        return {
            'statusCode': 400,
            'body': 'Function only supports input from events with a source type of: aws.ecs'
        }

    # Handle ECS Task State Change
    if event["detail-type"] == "ECS Task State Change":
        logger.info('[EVENT_PROCESSING] Processing ECS Task State Change event')
        db_service = DynamoDBService(
            config.task_table_name,
            config.job_table_name,
            config.node_table_name
        )
        ecs_service = ECSService(config.ecs_cluster_name)
        processor = TaskProcessor(db_service, ecs_service, config)
        processor.process_task_state_change(event["detail"])

    logger.info('[LAMBDA_COMPLETE] ECS Task Handler completed')
    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }
