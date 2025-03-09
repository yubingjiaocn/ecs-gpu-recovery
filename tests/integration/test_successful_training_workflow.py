#!/usr/bin/env python3
"""
Test script for ECS GPU Recovery workflow with successful training.

This script creates a test job with a training task that exits successfully after 120s
and puts the necessary records in DynamoDB to test the GPU recovery workflow.
"""

import boto3
import json
import time
import uuid
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

# Import task definition creators
from task_definitions import (
    create_timed_successful_training_task_definition
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize AWS clients
ecs_client = boto3.client('ecs')
dynamodb = boto3.resource('dynamodb')

class SuccessfulTrainingWorkflowTest:
    """Test class for ECS GPU Recovery workflow with successful training."""

    def __init__(self, cluster_name: str, task_table_name: str, job_table_name: str, node_table_name: str):
        """
        Initialize the test class.

        Args:
            cluster_name: ECS cluster name
            task_table_name: DynamoDB task table name
            job_table_name: DynamoDB job table name
            node_table_name: DynamoDB node table name
        """
        self.cluster_name = cluster_name
        self.task_table = dynamodb.Table(task_table_name)
        self.job_table = dynamodb.Table(job_table_name)
        self.node_table = dynamodb.Table(node_table_name)
        self.job_id = f"test-job-{uuid.uuid4().hex[:8]}"
        self.job_timestamp = datetime.now().isoformat()
        self.tasks = []
        self.container_instances = []

    def setup_task_definitions(self) -> str:
        """
        Set up the task definition for the test.

        Returns:
            str: ARN for timed successful training task definition
        """
        logger.info("Setting up task definition...")
        timed_successful_task_def_arn = create_timed_successful_training_task_definition()

        return timed_successful_task_def_arn

    def get_container_instances(self) -> List[Dict[str, Any]]:
        """
        Get available container instances in the cluster.

        Returns:
            List[Dict[str, Any]]: List of container instance details
        """
        logger.info(f"Getting container instances for cluster {self.cluster_name}...")
        response = ecs_client.list_container_instances(
            cluster=self.cluster_name,
            status='ACTIVE'
        )

        if not response.get('containerInstanceArns'):
            logger.error(f"No container instances found in cluster {self.cluster_name}")
            return []

        container_instances = ecs_client.describe_container_instances(
            cluster=self.cluster_name,
            containerInstances=response['containerInstanceArns']
        )['containerInstances']

        logger.info(f"Found {len(container_instances)} container instances")
        return container_instances

    def run_task(self, task_def_arn: str, container_instance_arn: str, node_index: int) -> Optional[Dict[str, Any]]:
        """
        Run a task on a container instance.

        Args:
            task_def_arn: Task definition ARN
            container_instance_arn: Container instance ARN
            node_index: Node index in the job

        Returns:
            Optional[Dict[str, Any]]: Task details if successful, None otherwise
        """
        try:
            logger.info(f"Running task {task_def_arn} on instance {container_instance_arn}")
            response = ecs_client.start_task(
                cluster=self.cluster_name,
                taskDefinition=task_def_arn,
                containerInstances=[container_instance_arn],
                startedBy=f"ecs-gpu-recovery-test-{self.job_id}",
                tags=[
                    {
                        'key': 'job_id',
                        'value': self.job_id
                    }
                ]
            )

            if not response.get('tasks'):
                logger.error(f"Failed to start task on instance {container_instance_arn}")
                return None

            task = response['tasks'][0]
            logger.info(f"Started task {task['taskArn']}")
            return task
        except Exception as e:
            logger.error(f"Error running task: {str(e)}")
            return None

    def create_dynamodb_records(self, tasks: List[Dict[str, Any]], container_instances: List[Dict[str, Any]]) -> bool:
        """
        Create DynamoDB records for the test job and tasks.

        Args:
            tasks: List of task details
            container_instances: List of container instance details

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create task records
            task_ids = []
            container_inst_ids = []
            assigned_nodes = []

            for i, task in enumerate(tasks):
                if not task:
                    continue

                task_id = task['taskArn'].split('/')[-1]
                task_ids.append(task_id)

                container_instance_arn = task['containerInstanceArn']
                container_inst_id = container_instance_arn.split('/')[-1]
                container_inst_ids.append(container_inst_id)

                # Find the corresponding container instance
                container_instance = next(
                    (ci for ci in container_instances if ci['containerInstanceArn'] == container_instance_arn),
                    None
                )

                if not container_instance:
                    logger.error(f"Container instance not found for ARN {container_instance_arn}")
                    continue

                # Get EC2 instance ID
                ec2_instance_id = container_instance['ec2InstanceId']
                node_name = f"node-{i}"
                assigned_nodes.append(node_name)

                # Create task record
                task_def_arn = task['taskDefinitionArn']
                task_def_parts = task_def_arn.split('/')
                task_def_name = task_def_parts[1].split(':')[0]
                task_def_revision = task_def_parts[1].split(':')[1]

                logger.info(f"Creating task record for task {task_id}")
                self.task_table.put_item(
                    Item={
                        'ecs_task_id': task_id,
                        'node_name': node_name,
                        'node_index_in_job': i,
                        'job_id': self.job_id,
                        'job_timestamp': self.job_timestamp,
                        'job_num_nodes': len(tasks),
                        'task_def_arn': task_def_arn,
                        'task_def_name': task_def_name,
                        'task_def_revision': task_def_revision,
                        'cluster_name': self.cluster_name,
                        'container_instance_arn': container_instance_arn,
                        'container_inst_id': container_inst_id,
                        'retry': '0',
                        'task_status': 'IN_PROGRESS',
                        'updated_at': datetime.now().isoformat(),
                        'created_at': datetime.now().isoformat()
                    }
                )

                # Create node record if it doesn't exist
                try:
                    self.node_table.put_item(
                        Item={
                            'node_name': node_name,
                            'container_instance_id': container_inst_id,
                            'container_instance_arn': container_instance_arn,
                            'cluster_name': self.cluster_name,
                            'node_status': 'IN_PROGRESS',
                            'ip': '10.0.0.1',  # Mock IP
                            'ibdev': 'mlx5_0',  # Mock IB device
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat()
                        },
                        ConditionExpression='attribute_not_exists(node_name)'
                    )
                    logger.info(f"Created node record for node {node_name}")
                except self.node_table.meta.client.exceptions.ConditionalCheckFailedException:
                    logger.info(f"Node record already exists for node {node_name}")
                    # Update node status
                    self.node_table.update_item(
                        Key={'node_name': node_name},
                        UpdateExpression='SET node_status = :val, updated_at = :time',
                        ExpressionAttributeValues={
                            ':val': 'IN_PROGRESS',
                            ':time': datetime.now().isoformat()
                        }
                    )

            # Create job record
            logger.info(f"Creating job record for job {self.job_id}")
            self.job_table.put_item(
                Item={
                    'job_id': self.job_id,
                    'job_timestamp': self.job_timestamp,
                    'cluster_name': self.cluster_name,
                    'num_nodes': len(tasks),
                    'assigned_nodes': assigned_nodes,
                    'submitted_container_inst_ids': container_inst_ids,
                    'submitted_ecs_task_ids': task_ids,
                    'updated_at': datetime.now().isoformat(),
                    'created_at': datetime.now().isoformat(),
                    'retry': '0',
                    'job_status': 'IN_PROGRESS'
                }
            )

            return True
        except Exception as e:
            logger.error(f"Error creating DynamoDB records: {str(e)}")
            return False

    def verify_dynamodb_records(self) -> bool:
        """
        Verify DynamoDB records for the test job and tasks.

        Returns:
            bool: True if records are valid, False otherwise
        """
        try:
            # Verify job record
            job_response = self.job_table.get_item(
                Key={'job_id': self.job_id}
            )

            if 'Item' not in job_response:
                logger.error(f"Job record not found for job {self.job_id}")
                return False

            job_record = job_response['Item']
            logger.info(f"Job record found: {job_record}")

            # Verify job status is updated correctly
            job_status = job_record.get('job_status')
            if job_status == 'IN_PROGRESS':
                logger.warning(f"Job status is still IN_PROGRESS, expected it to be updated")
            else:
                logger.info(f"Job status has been updated to {job_status}")

            # Verify task records
            for task in self.tasks:
                if not task:
                    continue

                task_id = task['taskArn'].split('/')[-1]
                task_response = self.task_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('ecs_task_id').eq(task_id)
                )

                if not task_response.get('Items'):
                    logger.error(f"Task record not found for task {task_id}")
                    return False

                task_record = task_response['Items'][0]
                logger.info(f"Task record found: {task_record}")

                # Verify task status is updated correctly
                task_status = task_record.get('task_status')
                if task_status == 'IN_PROGRESS':
                    logger.warning(f"Task status is still IN_PROGRESS, expected it to be updated")
                else:
                    logger.info(f"Task status has been updated to {task_status}")

                    # Verify task status matches ECS task status
                    ecs_response = ecs_client.describe_tasks(
                        cluster=self.cluster_name,
                        tasks=[task_id]
                    )

                    if ecs_response.get('tasks'):
                        containers = ecs_response['tasks'][0].get('containers', [])
                        for container in containers:
                            exit_code = container.get('exitCode')
                            if exit_code is not None:
                                expected_status = 'COMPLETE' if exit_code == 0 else 'FAILED'
                                if task_status != expected_status:
                                    logger.warning(f"Task {task_id} status {task_status} does not match expected status {expected_status}")
                                else:
                                    logger.info(f"Task {task_id} status {task_status} matches expected status {expected_status}")

                # Verify node records
                node_name = task_record['node_name']
                node_response = self.node_table.get_item(
                    Key={'node_name': node_name}
                )

                if 'Item' not in node_response:
                    logger.error(f"Node record not found for node {node_name}")
                    return False

                node_record = node_response['Item']
                logger.info(f"Node record found: {node_record}")

                # Verify node status is updated correctly
                node_status = node_record.get('node_status')
                if node_status == 'IN_PROGRESS':
                    logger.warning(f"Node status is still IN_PROGRESS, expected it to be updated")
                else:
                    logger.info(f"Node status has been updated to {node_status}")

            return True
        except Exception as e:
            logger.error(f"Error verifying DynamoDB records: {str(e)}")
            return False

    def verify_task_status(self) -> Dict[str, str]:
        """
        Verify the status of ECS tasks.

        Returns:
            Dict[str, str]: Dictionary mapping task IDs to their status
        """
        task_statuses = {}

        for task in self.tasks:
            if not task:
                continue

            task_id = task['taskArn'].split('/')[-1]
            response = ecs_client.describe_tasks(
                cluster=self.cluster_name,
                tasks=[task_id]
            )

            if not response.get('tasks'):
                logger.warning(f"Task {task_id} not found")
                task_statuses[task_id] = "NOT_FOUND"
                continue

            task_status = response['tasks'][0]['lastStatus']
            task_statuses[task_id] = task_status
            logger.info(f"Task {task_id} status: {task_status}")

        return task_statuses

    def run_test(self) -> bool:
        """
        Run the test workflow.

        Returns:
            bool: True if successful, False otherwise
        """
        # Set up task definition
        timed_successful_task_def_arn = self.setup_task_definitions()

        # Get container instances
        self.container_instances = self.get_container_instances()
        if not self.container_instances:
            logger.error("No container instances found")
            return False

        # Run task
        task = self.run_task(timed_successful_task_def_arn, self.container_instances[0]['containerInstanceArn'], 0)
        if not task:
            logger.error("Failed to start task")
            return False

        self.tasks.append(task)

        # Create DynamoDB records
        if not self.create_dynamodb_records(self.tasks, self.container_instances):
            return False

        # Verify DynamoDB records
        if not self.verify_dynamodb_records():
            logger.warning("DynamoDB record verification failed")

        logger.info(f"Test job {self.job_id} created successfully")
        logger.info("Monitoring task status...")

        # Monitor task status for a while
        for i in range(15):  # Monitor for 150 seconds (15 * 10)
            time.sleep(10)

            # Check task status
            task_statuses = self.verify_task_status()

            # Check if task has COMPLETE successfully
            for task_id, status in task_statuses.items():
                if status == "STOPPED":
                    # Get exit code
                    response = ecs_client.describe_tasks(
                        cluster=self.cluster_name,
                        tasks=[task_id]
                    )

                    if response.get('tasks'):
                        containers = response['tasks'][0].get('containers', [])
                        for container in containers:
                            exit_code = container.get('exitCode')
                            if exit_code is not None:
                                logger.info(f"Task {task_id} exited with code {exit_code}")

                                # Instead of updating DynamoDB directly, check if the original program updates it
                                logger.info(f"Task {task_id} COMPLETE with exit code {exit_code}. Waiting for system to update DynamoDB...")

        # Wait for the system to update DynamoDB records and then verify them
        logger.info("Waiting for system to update DynamoDB records...")
        max_wait_time = 60  # Maximum wait time in seconds
        wait_interval = 5   # Check every 5 seconds

        for i in range(max_wait_time // wait_interval):
            time.sleep(wait_interval)

            # Check if all task records have been updated by the system
            all_updated = True
            for task in self.tasks:
                if not task:
                    continue

                task_id = task['taskArn'].split('/')[-1]
                task_response = self.task_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('ecs_task_id').eq(task_id)
                )

                if not task_response.get('Items'):
                    logger.warning(f"Task record not found for task {task_id}")
                    all_updated = False
                    continue

                task_record = task_response['Items'][0]
                task_status = task_record.get('task_status')

                # Check if the task status has been updated from IN_PROGRESS
                if task_status == 'IN_PROGRESS':
                    logger.info(f"Task {task_id} status is still IN_PROGRESS, waiting for system to update...")
                    all_updated = False
                else:
                    logger.info(f"Task {task_id} status has been updated to {task_status}")

                    # Verify the task status matches the expected value based on exit code
                    ecs_response = ecs_client.describe_tasks(
                        cluster=self.cluster_name,
                        tasks=[task_id]
                    )

                    if ecs_response.get('tasks'):
                        containers = ecs_response['tasks'][0].get('containers', [])
                        for container in containers:
                            exit_code = container.get('exitCode')
                            if exit_code is not None:
                                expected_status = 'COMPLETE' if exit_code == 0 else 'FAILED'
                                if task_status != expected_status:
                                    logger.warning(f"Task {task_id} status {task_status} does not match expected status {expected_status}")
                                else:
                                    logger.info(f"Task {task_id} status {task_status} matches expected status {expected_status}")

            if all_updated:
                logger.info("All task records have been updated by the system")
                break

            logger.info(f"Waiting for system to update all task records... ({(i+1)*wait_interval}/{max_wait_time}s)")

        # Final verification of DynamoDB records
        if not self.verify_dynamodb_records():
            logger.warning("Final DynamoDB record verification failed")
            return False

        # Final verification of task status
        final_task_statuses = self.verify_task_status()
        logger.info(f"Final task statuses: {final_task_statuses}")

        logger.info("Test COMPLETE. Check CloudWatch logs and DynamoDB tables for results.")
        return True

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test ECS GPU Recovery workflow with successful training')
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

    test = SuccessfulTrainingWorkflowTest(
        cluster_name=args.cluster,
        task_table_name=args.task_table,
        job_table_name=args.job_table,
        node_table_name=args.node_table
    )

    success = test.run_test()
    if success:
        logger.info("Test workflow started successfully")
        logger.info(f"Job ID: {test.job_id}")
    else:
        logger.error("Test workflow failed")

if __name__ == "__main__":
    main()
