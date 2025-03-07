import os
from typing import Dict, Any

class Config:
    """
    Centralized configuration for the ECS GPU Recovery CDK Stack.
    Configuration can be overridden using environment variables.
    """

    # DynamoDB Tables
    TASK_TABLE_NAME = "ecs_task"
    JOB_TABLE_NAME = "ecs_job"
    NODE_TABLE_NAME = "ecs_node"

    # ECS Configuration
    ECS_CLUSTER_NAME = "nwcd-gpu-testing"
    DCGM_HEALTH_CHECK_TASK = "arn:aws:ecs:us-west-2:600413481647:task-definition/gpu-dcgm-health-check:2"

    # SNS Configuration
    SNS_TOPIC_NAME = "gpu-training-notifications"
    SNS_TOPIC_DISPLAY_NAME = "GPU Training Job Notifications"

    # Lambda Configuration
    LAMBDA_TIMEOUT_SECONDS = 60
    LAMBDA_MEMORY_SIZE = 256

    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        """
        Returns the configuration with environment variable overrides.

        Environment variables take precedence over default values.
        """
        config = {}

        # Get all class variables (excluding methods and private variables)
        for key in dir(cls):
            if not key.startswith('_') and not callable(getattr(cls, key)):
                # Check if environment variable override exists
                env_value = os.environ.get(key)
                if env_value is not None:
                    config[key] = env_value
                else:
                    config[key] = getattr(cls, key)

        return config
