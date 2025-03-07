import os
from common import logger

class Config:
    """Centralized configuration management"""

    def __init__(self):
        """Initialize configuration from environment variables"""
        self.task_table_name = os.environ.get('TASK_TABLE_NAME')
        self.job_table_name = os.environ.get('JOB_TABLE_NAME')
        self.node_table_name = os.environ.get('NODE_TABLE_NAME')
        self.ecs_cluster_name = os.environ.get('ECS_CLUSTER_NAME')
        self.dcgm_health_check_task = os.environ.get('DCGM_HEALTH_CHECK_TASK')

        # Validate required configuration
        missing = []
        if not self.task_table_name: missing.append('TASK_TABLE_NAME')
        if not self.job_table_name: missing.append('JOB_TABLE_NAME')
        if not self.node_table_name: missing.append('NODE_TABLE_NAME')
        if not self.ecs_cluster_name: missing.append('ECS_CLUSTER_NAME')
        if not self.dcgm_health_check_task: missing.append('DCGM_HEALTH_CHECK_TASK')

        if missing:
            logger.error(f"[CONFIG_ERROR] Missing required environment variables: {', '.join(missing)}")
