from common import logger

def validate_ecs_task_event(event):
    """
    Validates that the event is an ECS event.

    Args:
        event (dict): The event to validate

    Returns:
        bool: True if valid event, False otherwise
    """
    logger.info("[EVENT_VALIDATION] Validating event source")
    if event.get("source") != "aws.ecs":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports input from events with a source type of: aws.ecs")
        return False

    logger.info("[EVENT_VALIDATION_SUCCESS] Event source is valid")
    return True
