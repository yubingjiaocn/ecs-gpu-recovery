from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_ecs as ecs
)
from constructs import Construct
import os

class EcsGpuRecoveryStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create DynamoDB table for container instance status tracking
        container_instance_table = dynamodb.Table(
            self, "ContainerInstanceTable",
            partition_key=dynamodb.Attribute(
                name="container_inst_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name="ecs_container_instance"
        )

        # Create DynamoDB table for job tracking
        training_job_table = dynamodb.Table(
            self, "HybridGpuTrainingJobTable",
            partition_key=dynamodb.Attribute(
                name="job_id_rank",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name="ecs-job-sub"
        )

        # Lambda function for ECS task handling
        ecs_task_handler = lambda_.Function(
            self, "EcsTaskHandler",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/ecs_task_handler"),
            handler="ecs_task_handler.lambda_handler",
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "TRAINING_JOB_TABLE_NAME": training_job_table.table_name,
                "ECS_CLUSTER_NAME": "nwcd-gpu-testing",  # Updated ECS cluster name
                "DCGM_HEALTH_CHECK_TASK": "arn:aws:ecs:us-west-2:600413481647:task-definition/gpu-dcgm-health-check:1"
            },
            description="Lambda function to manage ECS GPU training tasks"
        )

        # Lambda function for DCGM task monitoring (Lambda 2)
        dcgm_task_monitor = lambda_.Function(
            self, "DcgmTaskMonitor",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/dcgm_task_monitor"),
            handler="dcgm_task_monitor.lambda_handler",
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "TRAINING_JOB_TABLE_NAME": training_job_table.table_name,
                "CONTAINER_INSTANCE_TABLE_NAME": container_instance_table.table_name,
                "ECS_CLUSTER_NAME": "nwcd-gpu-testing"
            },
            description="Lambda function to monitor DCGM task completions and manage recovery actions"
        )

        # Grant Lambda permissions to access DynamoDB tables
        training_job_table.grant_read_write_data(ecs_task_handler)
        training_job_table.grant_read_write_data(dcgm_task_monitor)
        container_instance_table.grant_read_write_data(dcgm_task_monitor)

        # Grant Lambda permissions to access ECS and SSM
        ecs_task_handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ecs:DescribeTasks",
                    "ecs:DescribeContainerInstances",
                    "ecs:StopTask",
                    "ecs:RunTask",
                    "ecs:ListTasks",
                    "ecs:StartTask",
                    "ecs:DescribeTaskDefinition",
                    "ecs:TagResource"
                ],
                resources=["*"]
            )
        )

        ecs_task_handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:DescribeInstanceInformation",
                    "ssm:SendCommand"
                ],
                resources=["*"]
            )
        )

        # Grant Lambda 2 permissions to access ECS and SSM
        dcgm_task_monitor.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ecs:DescribeTasks",
                    "ecs:DescribeContainerInstances",
                    "ecs:ListTasks"
                ],
                resources=["*"]
            )
        )

        dcgm_task_monitor.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:SendCommand"
                ],
                resources=["*"]
            )
        )

        # Create SNS topic for notifications
        notification_topic = sns.Topic(
            self, "GpuTrainingNotificationTopic",
            display_name="GPU Training Job Notifications",
            topic_name="gpu-training-notifications"
        )

        # Lambda function for ECS Instance Monitor (Lambda 3)
        ecs_instance_monitor = lambda_.Function(
            self, "EcsInstanceMonitor",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/ecs_instance_monitor"),
            handler="ecs_instance_monitor.lambda_handler",
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "TRAINING_JOB_TABLE_NAME": training_job_table.table_name,
                "ECS_CLUSTER_NAME": "nwcd-gpu-testing",
                "SNS_TOPIC_ARN": notification_topic.topic_arn
            },
            description="Lambda function to monitor ECS instance restart events and handle training jobs"
        )

        # Grant Lambda 3 permissions
        training_job_table.grant_read_write_data(ecs_instance_monitor)
        notification_topic.grant_publish(ecs_instance_monitor)

        ecs_instance_monitor.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ecs:DescribeContainerInstances",
                    "ecs:RunTask",
                    "ecs:ListTasks",
                    "ecs:StartTask",
                    "ecs:DescribeTasks",
                    "ecs:DescribeTaskDefinition"
                ],
                resources=["*"]
            )
        )

        # EventBridge rule to capture ECS task state change events (excluding DCGM tasks)
        ecs_task_event_rule = events.Rule(
            self, "EcsTaskEventRule",
            event_pattern=events.EventPattern(
                source=["aws.ecs"],
                detail_type=["ECS Task State Change"],
                detail={
                    "taskDefinitionArn": [{
                        "anything-but": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:task-definition/dcgm-*"
                    }],
                    "clusterArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/nwcd-gpu-testing"
                    }]
                }
            ),
            description="Rule to capture ECS task state change events (excluding DCGM tasks)"
        )

        # Add Lambda as target for EventBridge rule
        ecs_task_event_rule.add_target(
            targets.LambdaFunction(
                ecs_task_handler,
                event=events.RuleTargetInput.from_event_path("$")
            )
        )

        # EventBridge rule to capture DCGM task state change events (Lambda 2)
        dcgm_task_event_rule = events.Rule(
            self, "DcgmTaskEventRule",
            event_pattern=events.EventPattern(
                source=["aws.ecs"],
                detail_type=["ECS Task State Change"],
                detail={
                    "taskDefinitionArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:task-definition/gpu-dcgm-health-check"
                    }],
                    "clusterArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/nwcd-gpu-testing"
                    }]
                }
            ),
            description="Rule to capture DCGM task state change events"
        )

        # Add Lambda 2 as target for DCGM EventBridge rule
        dcgm_task_event_rule.add_target(
            targets.LambdaFunction(
                dcgm_task_monitor,
                event=events.RuleTargetInput.from_event_path("$")
            )
        )

        # EventBridge rule to capture ECS Container Instance State Change events (Lambda 3)
        ecs_instance_event_rule = events.Rule(
            self, "EcsInstanceEventRule",
            event_pattern=events.EventPattern(
                source=["aws.ecs"],
                detail_type=["ECS Container Instance State Change"],
                detail={
                    "clusterArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/nwcd-gpu-testing"
                    }]
                }
            ),
            description="Rule to capture ECS Container Instance State Change events"
        )

        # Add Lambda 3 as target for Container Instance EventBridge rule
        ecs_instance_event_rule.add_target(
            targets.LambdaFunction(
                ecs_instance_monitor,
                event=events.RuleTargetInput.from_event_path("$")
            )
        )
