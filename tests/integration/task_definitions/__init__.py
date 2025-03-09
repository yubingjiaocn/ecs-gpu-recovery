"""
Task definitions for ECS GPU Recovery test suite.
"""

from .failed_training_task import create_failed_training_task_definition
from .successful_training_task import create_successful_training_task_definition
from .mock_dcgm_task import create_mock_dcgm_task_definition
from .timed_successful_training_task import create_timed_successful_training_task_definition

__all__ = [
    'create_failed_training_task_definition',
    'create_successful_training_task_definition',
    'create_mock_dcgm_task_definition',
    'create_timed_successful_training_task_definition'
]
