import logging
import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class JobProcessor:
    """Processor for job-related operations"""

    def __init__(self, db_service, ecs_service, notification_service):
        """
        Initialize job processor.

        Args:
            db_service (DynamoDBService): DynamoDB service instance
            ecs_service (ECSService): ECS service instance
            notification_service (NotificationService): Notification service instance
        """
        logger.info("[PROCESSOR_INIT] Initializing job processor")
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.notification_service = notification_service
        logger.info("[PROCESSOR_INIT_COMPLETE] Job processor initialized")

    def process_job(self, job_id, container_inst_id):
        """
        Process a job based on its current state.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_PROCESS_START] Processing job {job_id} on instance {container_inst_id}")
        # Get job record
        job_record = self.db_service.get_job(job_id)
        if not job_record:
            logger.warning(f"[JOB_PROCESS_ERROR] No job record found for job ID {job_id}")
            return False

        job_status = job_record.get('job_status')
        retry = int(job_record.get('retry', 0))

        logger.info(f"[JOB_STATUS] Job: {job_id}, status: {job_status}, retry: {retry}")

        # Skip if job status is 'FAILED'
        if job_status == 'FAILED':
            logger.info(f"[JOB_SKIP] Job {job_id} status is 'FAILED', skipping")
            return False

        # Process based on retry count
        if retry == 0:
            logger.info(f"[JOB_RETRY_FIRST] Handling first retry for job {job_id}")
            return self._handle_first_retry(job_id, container_inst_id)
        else:
            logger.info(f"[JOB_RETRY_SUBSEQUENT] Handling subsequent retry for job {job_id}")
            return self._handle_subsequent_retry(job_id, container_inst_id)

    def _handle_first_retry(self, job_id, container_inst_id):
        """
        Handle first retry attempt for a job.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_RETRY_FIRST_START] Job {job_id} retry is 0, re-executing all related tasks")

        # Get job record to access submitted_ecs_task_ids
        job_record = self.db_service.get_job(job_id)
        if not job_record or 'submittd_ecs_task_ids' not in job_record:
            logger.warning(f"[JOB_RETRY_ERROR] No job record or task IDs found for job ID {job_id}")
            return False

        # Use submitted_ecs_task_ids from job table as the authority source
        task_ids = job_record.get('submittd_ecs_task_ids', [])
        if not task_ids:
            logger.warning(f"[JOB_RETRY_ERROR] No task IDs found in job record for job ID {job_id}")
            return False

        logger.info(f"[JOB_RETRY_TASKS] Found {len(task_ids)} tasks for job ID {job_id}")

        successful_tasks = 0
        new_task_ids = []

        # Run each task based on task IDs from job record
        for task_id in task_ids:
            # Get task record to access container instance and node information
            task_record = self.db_service.get_task(task_id)
            if not task_record:
                logger.warning(f"[TASK_ERROR] No task record found for task ID {task_id}")
                continue

            container_instance_arn = task_record.get('container_instance_arn')
            node_name = task_record.get('node_name')

            logger.info(f"[TASK_RETRY] Retrying task {task_id} for job {job_id}")

            # Get task information
            task_info = self.ecs_service.describe_task(task_id)
            if not task_info:
                logger.warning(f"[TASK_RETRY_ERROR] Could not get task information for {task_id}")
                continue

            # Set container instance status to IN_PROGRESS for retry
            if container_instance_arn:
                logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to IN_PROGRESS")
                self.ecs_service.set_instance_status(container_instance_arn, 'IN_PROGRESS')

                # Update node status if node name is available
                if node_name:
                    logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to IN_PROGRESS")
                    self.db_service.update_node_status(node_name, 'IN_PROGRESS')

            # Run the task
            response = self.ecs_service.run_task(task_info)

            if response and response.get('tasks'):
                # Get new task ID
                new_task_id = response['tasks'][0]["taskArn"].split('/')[-1]
                logger.info(f"[TASK_RETRY_SUCCESS] Task {task_id} restarted as {new_task_id}")

                # Set old task record to TERMINATED status
                self.db_service.update_task_status(task_id, 'TERMINATED')

                # Create new task record with the same information but new task ID
                self._create_new_task_record(task_record, new_task_id)

                # Add new task ID to the list for job record update
                new_task_ids.append(new_task_id)
                successful_tasks += 1
            else:
                logger.warning(f"[TASK_RETRY_FAILED] Failed to restart task {task_id}")

        # Update job retry count and task IDs
        if successful_tasks > 0:
            logger.info(f"[JOB_RETRY_UPDATE] Updating job {job_id} retry count to 1 and task IDs")
            self.db_service.job_table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET retry = :rt, submittd_ecs_task_ids = :tasks, updated_at = :time',
                ExpressionAttributeValues={
                    ':rt': '1',
                    ':tasks': new_task_ids,
                    ':time': datetime.datetime.now().isoformat()
                }
            )
            logger.info(f"[JOB_RETRY_UPDATED] Job {job_id} retry count updated to 1 and task IDs updated")

        logger.info(f"[JOB_RETRY_FIRST_COMPLETE] Successfully restarted {successful_tasks} tasks for job {job_id}")
        return successful_tasks > 0

    def _handle_subsequent_retry(self, job_id, container_inst_id):
        """
        Handle subsequent retry attempts for a job.

        Args:
            job_id (str): Job ID
            container_inst_id (str): Container instance ID

        Returns:
            bool: True if job was processed successfully, False otherwise
        """
        logger.info(f"[JOB_RETRY_SUBSEQUENT_START] Job {job_id} retry is not 0, updating all related tasks to 'FAILED'")

        # Get job record to access submitted_ecs_task_ids
        job_record = self.db_service.get_job(job_id)
        if not job_record or 'submittd_ecs_task_ids' not in job_record:
            logger.warning(f"[JOB_RETRY_ERROR] No job record or task IDs found for job ID {job_id}")
            return False

        # Use submitted_ecs_task_ids from job table as the authority source
        task_ids = job_record.get('submittd_ecs_task_ids', [])
        if not task_ids:
            logger.warning(f"[JOB_RETRY_ERROR] No task IDs found in job record for job ID {job_id}")
            return False

        logger.info(f"[JOB_RETRY_TASKS] Found {len(task_ids)} tasks for job {job_id}")

        # Update all task statuses to 'TERMINATED'
        for task_id in task_ids:
            self.db_service.update_task_status(task_id, 'TERMINATED')

            # Get task record to access container instance and node information
            task_record = self.db_service.get_task(task_id)
            if not task_record:
                continue

            container_instance_arn = task_record.get('container_instance_arn')
            node_name = task_record.get('node_name')
            task_container_inst_id = task_record.get('container_inst_id')

            if container_instance_arn:
                if task_container_inst_id == container_inst_id:
                    # Mark failed instance as FAILED
                    logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to FAILED")
                    self.ecs_service.set_instance_status(container_instance_arn, 'FAILED')

                    # Update node status if node name is available
                    if node_name:
                        logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to FAILED")
                        self.db_service.update_node_status(node_name, 'FAILED')
                else:
                    # Release other related instances to AVAILABLE
                    logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to AVAILABLE")
                    self.ecs_service.set_instance_status(container_instance_arn, 'AVAILABLE')

                    # Update node status if node name is available
                    if node_name:
                        logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to AVAILABLE")
                        self.db_service.update_node_status(node_name, 'AVAILABLE')

        # Update job status to FAILED
        self.db_service.update_job_status(job_id, 'FAILED')

        # Send notification to technical staff
        subject = f"Job {job_id} failed after instance restart"
        message = (f"Job {job_id} on instance {container_inst_id} failed after restart. "
                  f"All related tasks have been marked as terminated. Please investigate.")
        logger.info(f"[NOTIFICATION_PREPARE] Sending failure notification for job {job_id}")
        self.notification_service.send_notification(subject, message)

        logger.info(f"[JOB_RETRY_SUBSEQUENT_COMPLETE] Job {job_id} marked as failed after retry")
        return True
    def _create_new_task_record(self, original_task_record, new_task_id):
        """
        Create a new task record with updated task ID and status.

        Args:
            original_task_record (dict): Original task record
            new_task_id (str): New ECS task ID

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"[TASK_CREATE] Creating new task record for task ID {new_task_id}")

        # Create a copy of the original task record
        new_task_record = original_task_record.copy()

        # Update fields for the new task record
        new_task_record['ecs_task_id'] = new_task_id
        new_task_record['task_status'] = 'IN_PROGRESS'
        new_task_record['retry'] = str(int(original_task_record.get('retry', '0')) + 1)
        new_task_record['updated_at'] = datetime.datetime.now().isoformat()

        # Put the new task record in the task table
        self.db_service.task_table.put_item(Item=new_task_record)
        logger.info(f"[TASK_CREATE_COMPLETE] Created new task record for {new_task_id}")
        return True
