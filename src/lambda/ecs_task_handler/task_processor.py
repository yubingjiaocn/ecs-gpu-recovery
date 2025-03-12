from common import logger

class TaskProcessor:
    """Processor for task-related operations"""

    def __init__(self, db_service, ecs_service, config):
        """
        Initialize task processor.

        Args:
            db_service (DynamoDBService): DynamoDB service instance
            ecs_service (ECSService): ECS service instance
            config (Config): Configuration
        """
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.config = config

    def process_task_state_change(self, detail):
        """
        Process ECS Task State Change event.

        Args:
            detail (dict): Event detail

        Returns:
            bool: True if processed successfully, False otherwise
        """
        # Check if task is STOPPED
        if detail.get('lastStatus') != 'STOPPED':
            logger.info(f"[TASK_STATE] Task status is {detail.get('lastStatus')}, no action needed")
            return False

        task_id = detail['taskArn'].split('/')[2]
        cluster_arn = detail['clusterArn']
        cluster_name = cluster_arn.split('/')[1]

        logger.info(f"[TASK_STATE_CHANGE] Processing stopped task {task_id} in cluster {cluster_name}")

        # Get task detail
        task_detail = self.ecs_service.describe_task(task_id)
        if not task_detail:
            logger.error(f"[TASK_DETAIL_ERROR] Could not get details for task {task_id}")
            return False

        stop_code = task_detail.get('stopCode')
        logger.info(f"[TASK_STOP_CODE] Task {task_id} stop code: {stop_code}")

        # Check if task was stopped by user
        if stop_code and stop_code.startswith('UserInitiated'):
            logger.info(f"[TASK_USER_STOPPED] Task {task_id} was stopped by user")
            return self.handle_user_stopped_task(task_id)
        else:
            # Get exit code from containers
            exit_code = self._get_container_exit_code(task_detail)
            logger.info(f"[TASK_EXIT_CODE] Task {task_id} exit code: {exit_code}")

            if exit_code == 0:
                return self.handle_task_exit_code_0(task_id)
            else:
                return self.handle_task_exit_code_1(task_id, cluster_arn, task_detail)

        return False

    def _get_container_exit_code(self, task_detail):
        """
        Get exit code from task containers.

        Args:
            task_detail (dict): Task detail

        Returns:
            int or None: Exit code if found, None otherwise
        """
        containers = task_detail.get('containers', [])
        for container in containers:
            if container.get('exitCode') is not None:
                return container.get('exitCode')
        return None

    def _extract_task_ids_from_job(self, job_record):
        """
        Extract task IDs from job record's submitted_ecs_task_ids field.

        Args:
            job_record (dict): Job record

        Returns:
            list: List of task IDs
        """
        if not job_record:
            return []

        submitted_ecs_task_ids = job_record.get('submitted_ecs_task_ids', '')
        if isinstance(submitted_ecs_task_ids, str):
            # If it's stored as a comma-separated string
            return [task_id.strip() for task_id in submitted_ecs_task_ids.split(',') if task_id.strip()]
        elif isinstance(submitted_ecs_task_ids, list):
            # If it's stored as a list
            return submitted_ecs_task_ids

        return []

    def _get_job_info(self, task_id, context_name):
        """
        Get job information for a task and validate it exists.

        Args:
            task_id (str): ECS task ID
            context_name (str): Context name for logging (e.g., 'TASK_STOP', 'TASK_COMPLETE')

        Returns:
            tuple: (job_id, job_record, task_records) or (None, None, None) if not found
        """
        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[{context_name}] No job found for task {task_id}")
            return None, None, None
        return job_id, job_record, task_records

    def _are_all_tasks_in_state(self, task_ids, current_task_id, expected_state='STOPPED'):
        """
        Check if all tasks for a job are in the expected state.

        Args:
            task_ids (list): List of task IDs
            current_task_id (str): Current task ID (already known to be in the expected state)
            expected_state (str): Expected task state to check for

        Returns:
            bool: True if all tasks are in the expected state
        """
        for task_id_from_job in task_ids:
            if task_id_from_job != current_task_id:
                # Check if this task is in the expected state
                task_detail = self.ecs_service.describe_task(task_id_from_job)
                if task_detail and task_detail.get('lastStatus') != expected_state:
                    return False
        return True

    def _update_job_and_instances(self, job_id, task_id, job_record, task_records, job_status,
                                 instance_status='AVAILABLE', only_in_progress=False):
        """
        Update job status and related instance statuses.

        Args:
            job_id (str): Job ID
            task_id (str): Task ID
            job_record (dict): Job record
            task_records (list): Task records
            job_status (str): Status to set for the job
            instance_status (str): Status to set for instances
            only_in_progress (bool): If True, only update instances in IN_PROGRESS state

        Returns:
            bool: True if successful
        """
        logger.info(f"[JOB_STATE_CHANGE] Updating job {job_id} status to {job_status}")
        self.db_service.update_job_status(job_id, job_status)

        # Get task detail for the current task
        task_detail = self.ecs_service.describe_task(task_id)
        if task_detail:
            # Set related instances to specified status
            logger.info(f"[INSTANCE_STATE_CHANGE] Setting related instances to {instance_status} for job {job_id}")
            cluster_arn = task_detail.get('clusterArn')
            self._set_related_instances_status(task_records, task_detail, cluster_arn, instance_status, only_in_progress)

        return True

    def _get_node_name_for_instance(self, task_records, container_instance_arn):
        """
        Get node name from task records for a container instance ARN.

        Args:
            task_records (list): List of task records
            container_instance_arn (str): Container instance ARN

        Returns:
            str or None: Node name if found, None otherwise
        """
        for task in task_records:
            if task.get('container_instance_arn') == container_instance_arn:
                return task.get('node_name')
        return None

    def handle_user_stopped_task(self, task_id):
        """
        Handle task stopped by user.

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info(f"[TASK_STOP] Task {task_id} was stopped by user")

        job_id, job_record, task_records = self._get_job_info(task_id, "TASK_STOP")
        if not job_id:
            return False

        # Update task status to TERMINATED
        self.db_service.update_task_status(task_id, 'TERMINATED')

        # Get all task IDs from job record
        task_ids = self._extract_task_ids_from_job(job_record)

        # Check if all tasks for this job are stopped
        all_stopped = self._are_all_tasks_in_state(task_ids, task_id, 'STOPPED')

        # If all tasks are stopped, update job status
        if all_stopped and job_record and job_record.get('job_status') == 'IN_PROGRESS':
            return self._update_job_and_instances(
                job_id, task_id, job_record, task_records, 'USER_STOPPED', 'AVAILABLE', False
            )

        return True

    def handle_task_exit_code_0(self, task_id):
        """
        Handle task with exit code 0 (success).

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info(f"[TASK_COMPLETE] Task {task_id} exited with code 0")

        job_id, job_record, task_records = self._get_job_info(task_id, "TASK_COMPLETE")
        if not job_id:
            return False

        # Update task status to COMPLETE
        self.db_service.update_task_status(task_id, 'COMPLETE')

        # Get all task IDs from job record
        task_ids = self._extract_task_ids_from_job(job_record)

        # Check if all tasks for this job are complete
        all_complete = self._are_all_tasks_in_state(task_ids, task_id, 'STOPPED')

        # If all tasks are complete, update job status
        if all_complete and job_record:
            return self._update_job_and_instances(
                job_id, task_id, job_record, task_records, 'COMPLETE', 'AVAILABLE', False
            )

        return True

    def handle_task_exit_code_1(self, task_id, cluster_arn, task_detail):
        """
        Handle task with exit code other then 0 (error).

        Args:
            task_id (str): ECS task ID
            cluster_arn (str): ECS cluster ARN
            task_detail (dict): Task detail

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info(f"[TASK_FAILED] Task {task_id} exited with code other then 0")

        job_id, job_record, task_records = self._get_job_info(task_id, "TASK_FAILED")
        if not job_id:
            return False

        # Update task status to TERMINATED
        self.db_service.update_task_status(task_id, 'TERMINATED')

        # Get all task IDs from job record (authoritative source)
        task_ids = self._extract_task_ids_from_job(job_record)
        logger.info(f"[TASK_BATCH] Found {len(task_ids)} tasks for job {job_id}")

        # Stop all tasks for this job
        logger.info(f"[TASK_BATCH_STOP] Stopping all tasks for job {job_id}")
        self.ecs_service.stop_tasks(task_ids)

        # Set related instances to PENDING and mark tasks as TERMINATED
        logger.info(f"[INSTANCE_STATE_CHANGE] Setting related instances to PENDING for job {job_id}")
        self._set_related_instances_status(task_records, task_detail, cluster_arn, 'PENDING')

        # Get container instance ARNs
        container_instance_arns = self.ecs_service.get_container_instances_from_tasks(task_ids)
        logger.info(f"[HEALTH_CHECK_PREPARE] Running health checks on {len(container_instance_arns)} instances")

        # Run GPU DCGM check task on each container instance
        for container_instance_arn in container_instance_arns:
            # Get node name from task records
            node_name = self._get_node_name_for_instance(task_records, container_instance_arn)

            # Run DCGM health check
            self.ecs_service.run_dcgm_health_check(
                cluster_arn,
                container_instance_arn,
                job_id,
                self.config.dcgm_health_check_task
            )

            # Update node status if node name is found
            if node_name:
                self.db_service.update_node_status(node_name, 'PENDING_HEALTHCHECK')

        # Update job status to PENDING_HEALTHCHECK
        if job_record:
            logger.info(f"[JOB_STATE_CHANGE] Updating job {job_id} status to PENDING_HEALTHCHECK")
            self.db_service.update_job_status(job_id, 'PENDING_HEALTHCHECK')

        return True

    def _set_related_instances_status(self, task_records, task_detail, cluster_arn, status, only_in_progress=False):
        """
        Set related instances to the specified status.

        Args:
            task_records (list): List of task records
            task_detail (dict): Task detail
            cluster_arn (str): Cluster ARN
            status (str): Status to set (e.g., 'PENDING', 'AVAILABLE')
            only_in_progress (bool): If True, only update instances in IN_PROGRESS state

        Returns:
            bool: True if successful
        """
        current_instance_arn = task_detail.get('containerInstanceArn')
        logger.info(f"[INSTANCE_STATE_CHANGE] Current instance ARN: {current_instance_arn}")

        for record in task_records:
            container_instance_arn = record.get('container_instance_arn')
            node_name = record.get('node_name')
            task_id = record.get('ecs_task_id')
            node_status = record.get('node_status')

            # Skip if we're only updating IN_PROGRESS instances and this one isn't
            if only_in_progress and node_status != 'IN_PROGRESS':
                logger.info(f"[INSTANCE_STATE_CHANGE] Skipping instance {container_instance_arn} with status {node_status} (only updating IN_PROGRESS instances)")
                continue

            # For PENDING status, skip the current instance
            if status == 'PENDING' and container_instance_arn == current_instance_arn:
                continue

            if container_instance_arn:
                # Update ECS container instance status
                logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to {status}")
                self.ecs_service.set_instance_status(
                    container_instance_arn,
                    status,
                    cluster_arn
                )

                # Update node status in DynamoDB
                if node_name:
                    logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} from {node_status} to {status}")
                    self.db_service.update_node_status(node_name, status)

            # Mark task as TERMINATED if we're setting to PENDING
            if status == 'PENDING' and task_id:
                logger.info(f"[TASK_STATE_CHANGE] Setting task {task_id} to TERMINATED")
                self.db_service.update_task_status(task_id, 'TERMINATED')

        return True
