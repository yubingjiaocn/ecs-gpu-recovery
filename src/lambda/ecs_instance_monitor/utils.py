import logging
from functools import wraps

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def error_handler(func):
    """
    Decorator for consistent error handling across functions.

    Args:
        func: The function to wrap with error handling

    Returns:
        The wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"[ERROR] Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

def validate_ecs_container_instance_event(event):
    """
    Validates that the event is an ECS Container Instance State Change event.

    Args:
        event (dict): The event to validate

    Returns:
        bool: True if valid event, False otherwise
    """
    logger.info("[EVENT_VALIDATION] Validating event source and type")
    if event.get("source") != "aws.ecs":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports input from events with a source type of: aws.ecs")
        return False

    if event.get("detail-type") != "ECS Container Instance State Change":
        logger.error("[EVENT_VALIDATION_FAILED] Function only supports ECS Container Instance State Change events")
        return False

    logger.info("[EVENT_VALIDATION_SUCCESS] Event source and type are valid")
    return True
