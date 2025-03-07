#!/usr/bin/env python3
"""
Test runner for ECS GPU Recovery integration tests.
This script provides a convenient way to run the integration tests.
"""

import argparse
import logging
import sys
import os
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run ECS GPU Recovery integration tests')
    parser.add_argument('--cluster', type=str, default='nwcd-gpu-testing',
                        help='ECS cluster name')
    parser.add_argument('--task-table', type=str, default='ecs_task',
                        help='DynamoDB task table name')
    parser.add_argument('--job-table', type=str, default='ecs_job',
                        help='DynamoDB job table name')
    parser.add_argument('--node-table', type=str, default='ecs_node',
                        help='DynamoDB node table name')
    parser.add_argument('--setup-only', action='store_true',
                        help='Only set up task definitions, do not run tests')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    return parser.parse_args()

def setup_task_definitions():
    """Set up task definitions for testing."""
    logger.info("Setting up task definitions...")

    from task_definitions import (
        create_failed_training_task_definition,
        create_successful_training_task_definition,
        create_mock_dcgm_task_definition
    )

    failed_task_def_arn = create_failed_training_task_definition()
    successful_task_def_arn = create_successful_training_task_definition()
    dcgm_task_def_arn = create_mock_dcgm_task_definition()

    logger.info("Task definitions set up successfully:")
    logger.info(f"  Failed training task: {failed_task_def_arn}")
    logger.info(f"  Successful training task: {successful_task_def_arn}")
    logger.info(f"  DCGM health check task: {dcgm_task_def_arn}")

    return failed_task_def_arn, successful_task_def_arn, dcgm_task_def_arn

def run_gpu_recovery_test(args):
    """Run the GPU recovery test."""
    logger.info("Running GPU recovery test...")

    from test_gpu_recovery_workflow import EcsGpuRecoveryTest

    test = EcsGpuRecoveryTest(
        cluster_name=args.cluster,
        task_table_name=args.task_table,
        job_table_name=args.job_table,
        node_table_name=args.node_table
    )

    success = test.run_test()
    if success:
        logger.info("Test workflow started successfully")
        logger.info(f"Job ID: {test.job_id}")
        return 0
    else:
        logger.error("Test workflow failed")
        return 1

def main():
    """Main function."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting ECS GPU Recovery integration tests")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")

    # Make sure we're in the right directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Set up task definitions
    try:
        setup_task_definitions()
    except Exception as e:
        logger.error(f"Failed to set up task definitions: {str(e)}")
        return 1

    if args.setup_only:
        logger.info("Setup complete. Exiting as requested.")
        return 0

    # Run the test
    try:
        return run_gpu_recovery_test(args)
    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
