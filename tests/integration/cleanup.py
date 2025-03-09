#!/usr/bin/env python3
"""
Cleanup script for ECS GPU Recovery test suite.

This script:
1. Stops all tasks in the ECS Cluster
2. Waits for 20s for all events to finish running
3. Truncates jobs and tasks tables
4. Changes all node status to available

Usage:
    python cleanup.py [--cluster CLUSTER_NAME] [--task-table TASK_TABLE] [--job-table JOB_TABLE] [--node-table NODE_TABLE]
"""

import boto3
import time
import argparse
import logging
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ECSGPURecoveryCleanup:
    """Cleanup class for ECS GPU Recovery test suite."""

    def __init__(self, cluster_name: str, task_table_name: str, job_table_name: str, node_table_name: str):
        """
        Initialize the cleanup class.

        Args:
            cluster_name: ECS cluster name
            task_table_name: DynamoDB task table name
            job_table_name: DynamoDB job table name
            node_table_name: DynamoDB node table name
        """
        self.cluster_name = cluster_name
        self.ecs_client = boto3.client('ecs')
        self.dynamodb = boto3.resource('dynamodb')
        self.task_table = self.dynamodb.Table(task_table_name)
        self.job_table = self.dynamodb.Table(job_table_name)
        self.node_table = self.dynamodb.Table(node_table_name)

    def stop_all_tasks(self) -> None:
        """
        Stop all running tasks in the ECS cluster.
        """
        logger.info(f"Stopping all tasks in cluster {self.cluster_name}...")

        # List all tasks in the cluster
        response = self.ecs_client.list_tasks(
            cluster=self.cluster_name,
            desiredStatus='RUNNING'
        )

        task_arns = response.get('taskArns', [])

        if not task_arns:
            logger.info("No running tasks found in the cluster.")
            return

        logger.info(f"Found {len(task_arns)} running tasks. Stopping them...")

        # Stop each task
        for task_arn in task_arns:
            task_id = task_arn.split('/')[-1]
            try:
                self.ecs_client.stop_task(
                    cluster=self.cluster_name,
                    task=task_id,
                    reason='Cleanup script execution'
                )
                logger.info(f"Stopped task {task_id}")
            except Exception as e:
                logger.error(f"Failed to stop task {task_id}: {str(e)}")

        logger.info("All tasks have been stopped.")

    def wait_for_events(self, seconds: int = 20) -> None:
        """
        Wait for the specified number of seconds to allow events to complete.

        Args:
            seconds: Number of seconds to wait
        """
        logger.info(f"Waiting for {seconds} seconds to allow events to complete...")
        time.sleep(seconds)
        logger.info("Wait completed.")

    def truncate_dynamodb_table(self, table) -> None:
        """
        Truncate a DynamoDB table by scanning all items and deleting them.

        Args:
            table: DynamoDB table resource
        """
        table_name = table.table_name
        logger.info(f"Truncating table {table_name}...")

        # Get the table's key schema
        key_schema = table.key_schema
        key_names = [key['AttributeName'] for key in key_schema]

        # Scan all items
        scan_response = table.scan()
        items = scan_response.get('Items', [])

        if not items:
            logger.info(f"Table {table_name} is already empty.")
            return

        logger.info(f"Found {len(items)} items in table {table_name}. Deleting them...")

        # Delete each item
        deleted_count = 0
        for item in items:
            key = {key_name: item[key_name] for key_name in key_names}
            try:
                table.delete_item(Key=key)
                deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete item {key} from table {table_name}: {str(e)}")

        logger.info(f"Deleted {deleted_count} items from table {table_name}.")

    def update_all_nodes_to_available(self) -> None:
        """
        Update all nodes in the node table to 'AVAILABLE' status.
        """
        logger.info("Updating all nodes to AVAILABLE status...")

        # Scan all nodes
        scan_response = self.node_table.scan()
        nodes = scan_response.get('Items', [])

        if not nodes:
            logger.info("No nodes found in the node table.")
            return

        logger.info(f"Found {len(nodes)} nodes. Updating their status...")

        # Update each node
        updated_count = 0
        for node in nodes:
            node_name = node.get('node_name')
            if not node_name:
                logger.warning("Found node without node_name, skipping...")
                continue

            try:
                self.node_table.update_item(
                    Key={'node_name': node_name},
                    UpdateExpression='SET node_status = :val, updated_at = :time',
                    ExpressionAttributeValues={
                        ':val': 'AVAILABLE',
                        ':time': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ', time.gmtime())
                    }
                )
                updated_count += 1
                logger.info(f"Updated node {node_name} to AVAILABLE status")
            except Exception as e:
                logger.error(f"Failed to update node {node_name}: {str(e)}")

        logger.info(f"Updated {updated_count} nodes to AVAILABLE status.")

    def run_cleanup(self) -> None:
        """
        Run the complete cleanup process.
        """
        logger.info("Starting ECS GPU Recovery cleanup process...")

        # Step 1: Stop all tasks
        self.stop_all_tasks()

        # Step 2: Wait for events to complete
        self.wait_for_events()

        # Step 3: Truncate jobs and tasks tables
        self.truncate_dynamodb_table(self.job_table)
        self.truncate_dynamodb_table(self.task_table)

        # Step 4: Update all nodes to AVAILABLE
        self.update_all_nodes_to_available()

        logger.info("Cleanup process completed successfully.")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Cleanup ECS GPU Recovery test environment')
    parser.add_argument('--cluster', type=str, default='nwcd-gpu-testing',
                        help='ECS cluster name')
    parser.add_argument('--task-table', type=str, default='ecs_task',
                        help='DynamoDB task table name')
    parser.add_argument('--job-table', type=str, default='ecs_job',
                        help='DynamoDB job table name')
    parser.add_argument('--node-table', type=str, default='ecs_node',
                        help='DynamoDB node table name')
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_args()

    cleanup = ECSGPURecoveryCleanup(
        cluster_name=args.cluster,
        task_table_name=args.task_table,
        job_table_name=args.job_table,
        node_table_name=args.node_table
    )

    cleanup.run_cleanup()

if __name__ == "__main__":
    main()
