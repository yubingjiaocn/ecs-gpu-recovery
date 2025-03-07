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
        logger.info("[PROCESSOR_INIT] Initializing task processor")
        self.db_service = db_service
        self.ecs_service = ecs_service
        self.config = config
        logger.info("[PROCESSOR_INIT_COMPLETE] Task processor initialized")

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

            if exit_code == 1:
                return self.handle_task_exit_code_1(task_id, cluster_arn, task_detail)
            elif exit_code == 0:
                return self.handle_task_exit_code_0(task_id)
            else:
                logger.info(f"[TASK_UNHANDLED_CODE] Unhandled exit code: {exit_code}, no action taken")
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

    def handle_user_stopped_task(self, task_id):
        """
        Handle task stopped by user.

        Args:
            task_id (str): ECS task ID

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info(f"[TASK_STOP] Task {task_id} was stopped by user")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_STOP] No job found for task {task_id}")
            return False

        # Update task status to TERMINATED
        self.db_service.update_task_status(task_id, 'TERMINATED')

        # Get all task IDs from job record
        task_ids = self._extract_task_ids_from_job(job_record)

        # Check if all tasks for this job are stopped
        all_stopped = True
        for task_id_from_job in task_ids:
            if task_id_from_job != task_id:
                # Check if this task is still running
                task_detail = self.ecs_service.describe_task(task_id_from_job)
                if task_detail and task_detail.get('lastStatus') != 'STOPPED':
                    all_stopped = False
                    break

        # If all tasks are stopped, update job status
        if all_stopped and job_record:
            logger.info(f"[JOB_STATE_CHANGE] All tasks for job {job_id} are stopped, updating job status")
            self.db_service.update_job_status(job_id, 'USER_STOPPED')

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

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_COMPLETE] No job found for task {task_id}")
            return False

        # Update task status to COMPLETE
        self.db_service.update_task_status(task_id, 'COMPLETE')

        # Get all task IDs from job record
        task_ids = self._extract_task_ids_from_job(job_record)

        # Check if all tasks for this job are complete
        all_complete = True
        for task_id_from_job in task_ids:
            if task_id_from_job != task_id:
                # Check if this task is still running or not complete
                task_detail = self.ecs_service.describe_task(task_id_from_job)
                if task_detail and task_detail.get('lastStatus') != 'STOPPED':
                    all_complete = False
                    break

        # If all tasks are complete, update job status
        if all_complete and job_record:
            logger.info(f"[JOB_STATE_CHANGE] All tasks for job {job_id} are complete, updating job status")
            self.db_service.update_job_status(job_id, 'COMPLETE')

        return True

    def handle_task_exit_code_1(self, task_id, cluster_arn, task_detail):
        """
        Handle task with exit code 1 (error).

        Args:
            task_id (str): ECS task ID
            cluster_arn (str): ECS cluster ARN
            task_detail (dict): Task detail

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info(f"[TASK_FAILED] Task {task_id} exited with code 1")

        job_id, job_record, task_records = self.db_service.get_job_by_task_id(task_id)
        if not job_id:
            logger.warning(f"[TASK_FAILED] No job found for task {task_id}")
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
        self._set_related_instances_pending(task_records, task_detail, cluster_arn)

        # Get container instance ARNs
        container_instance_arns = self.ecs_service.get_container_instances_from_tasks(task_ids)
        logger.info(f"[HEALTH_CHECK_PREPARE] Running health checks on {len(container_instance_arns)} instances")

        # Run GPU DCGM check task on each container instance
        for container_instance_arn in container_instance_arns:
            # Get node name from task records
            node_name = None
            for task in task_records:
                if task.get('container_instance_arn') == container_instance_arn:
                    node_name = task.get('node_name')
                    break

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

        # Update job status
        if job_record:
            logger.info(f"[JOB_STATE_CHANGE] Updating job {job_id} status to FAILED")
            self.db_service.update_job_status(job_id, 'FAILED')

        return True

    def _set_related_instances_pending(self, task_records, task_detail, cluster_arn):
        """
        Set related instances to PENDING status.

        Args:
            task_records (list): List of task records
            task_detail (dict): Task detail
            cluster_arn (str): Cluster ARN

        Returns:
            bool: True if successful
        """
        current_instance_arn = task_detail.get('containerInstanceArn')
        logger.info(f"[INSTANCE_STATE_CHANGE] Current instance ARN: {current_instance_arn}")

        for record in task_records:
            container_instance_arn = record.get('container_instance_arn')
            node_name = record.get('node_name')
            task_id = record.get('ecs_task_id')

            if container_instance_arn and container_instance_arn != current_instance_arn:
                # Update ECS container instance status
                logger.info(f"[INSTANCE_STATE_CHANGE] Setting instance {container_instance_arn} to PENDING")
                self.ecs_service.set_instance_status(
                    container_instance_arn,
                    'PENDING',
                    cluster_arn
                )

                # Update node status in DynamoDB
                if node_name:
                    logger.info(f"[NODE_STATE_CHANGE] Setting node {node_name} to PENDING")
                    self.db_service.update_node_status(node_name, 'PENDING')

            # Mark task as TERMINATED
            if task_id:
                logger.info(f"[TASK_STATE_CHANGE] Setting task {task_id} to TERMINATED")
                self.db_service.update_task_status(task_id, 'TERMINATED')

        return True
