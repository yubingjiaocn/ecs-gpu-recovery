from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_fsx as fsx,
    CfnOutput
)
from constructs import Construct
import os
from ecs_gpu_recovery.config import Config

class EcsGpuRecoveryStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load configuration
        config = Config.get_config()

        # Create DynamoDB table for task tracking (individual ECS tasks)
        task_table = dynamodb.Table(
            self, "TaskTable",
            partition_key=dynamodb.Attribute(
                name="ecs_task_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name=config["TASK_TABLE_NAME"]
        )

        # Create DynamoDB table for job tracking (collection of tasks)
        job_table = dynamodb.Table(
            self, "JobTable",
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name=config["JOB_TABLE_NAME"]
        )

        # Create DynamoDB table for node information tracking
        node_table = dynamodb.Table(
            self, "NodeTable",
            partition_key=dynamodb.Attribute(
                name="node_name",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name=config["NODE_TABLE_NAME"]
        )

        # Lambda function for ECS task handling
        ecs_task_handler = lambda_.Function(
            self, "EcsTaskHandler",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/ecs_task_handler"),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(int(config["LAMBDA_TIMEOUT_SECONDS"])),
            memory_size=int(config["LAMBDA_MEMORY_SIZE"]),
            environment={
                "TASK_TABLE_NAME": task_table.table_name,
                "JOB_TABLE_NAME": job_table.table_name,
                "NODE_TABLE_NAME": node_table.table_name,
                "ECS_CLUSTER_NAME": config["ECS_CLUSTER_NAME"],
                "DCGM_HEALTH_CHECK_TASK": config["DCGM_HEALTH_CHECK_TASK"]
            },
            description="Lambda function to manage ECS GPU training tasks"
        )

        # Lambda function for DCGM task monitoring (Lambda 2)
        dcgm_task_monitor = lambda_.Function(
            self, "DcgmTaskMonitor",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/dcgm_task_monitor"),
            handler="dcgm_task_monitor.lambda_handler",
            timeout=Duration.seconds(int(config["LAMBDA_TIMEOUT_SECONDS"])),
            memory_size=int(config["LAMBDA_MEMORY_SIZE"]),
            environment={
                "TASK_TABLE_NAME": task_table.table_name,
                "JOB_TABLE_NAME": job_table.table_name,
                "NODE_TABLE_NAME": node_table.table_name,
                "ECS_CLUSTER_NAME": config["ECS_CLUSTER_NAME"]
            },
            description="Lambda function to monitor DCGM task completions and manage recovery actions"
        )

        # Grant Lambda permissions to access DynamoDB tables
        task_table.grant_read_write_data(ecs_task_handler)
        job_table.grant_read_write_data(ecs_task_handler)
        node_table.grant_read_write_data(ecs_task_handler)

        task_table.grant_read_write_data(dcgm_task_monitor)
        job_table.grant_read_write_data(dcgm_task_monitor)
        node_table.grant_read_write_data(dcgm_task_monitor)

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
                    "ecs:TagResource",
                    "ecs:PutAttributes",
                    "ecs:GetAttributes"
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
                    "ecs:ListTasks",
                    "ecs:PutAttributes",
                    "ecs:GetAttributes"
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
            display_name=config["SNS_TOPIC_DISPLAY_NAME"],
            topic_name=config["SNS_TOPIC_NAME"]
        )

        # Lambda function for ECS Instance Monitor (Lambda 3)
        ecs_instance_monitor = lambda_.Function(
            self, "EcsInstanceMonitor",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("src/lambda/ecs_instance_monitor"),
            handler="ecs_instance_monitor.lambda_handler",
            timeout=Duration.seconds(int(config["LAMBDA_TIMEOUT_SECONDS"])),
            memory_size=int(config["LAMBDA_MEMORY_SIZE"]),
            environment={
                "TASK_TABLE_NAME": task_table.table_name,
                "JOB_TABLE_NAME": job_table.table_name,
                "NODE_TABLE_NAME": node_table.table_name,
                "ECS_CLUSTER_NAME": config["ECS_CLUSTER_NAME"],
                "SNS_TOPIC_ARN": notification_topic.topic_arn
            },
            description="Lambda function to monitor ECS instance restart events and handle training jobs"
        )

        # Grant Lambda 3 permissions
        task_table.grant_read_write_data(ecs_instance_monitor)
        job_table.grant_read_write_data(ecs_instance_monitor)
        node_table.grant_read_write_data(ecs_instance_monitor)
        notification_topic.grant_publish(ecs_instance_monitor)

        ecs_instance_monitor.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ecs:DescribeContainerInstances",
                    "ecs:RunTask",
                    "ecs:ListTasks",
                    "ecs:StartTask",
                    "ecs:DescribeTasks",
                    "ecs:DescribeTaskDefinition",
                    "ecs:PutAttributes",
                    "ecs:GetAttributes"
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
                        "anything-but": config["DCGM_HEALTH_CHECK_TASK"]
                    }],
                    "clusterArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/{config['ECS_CLUSTER_NAME']}"
                    }],
                    "lastStatus": ["STOPPED"]
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
                        "prefix": config["DCGM_HEALTH_CHECK_TASK"]
                    }],
                    "clusterArn": [{
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/{config['ECS_CLUSTER_NAME']}"
                    }],
                    "lastStatus": ["STOPPED"]
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
                        "prefix": f"arn:{Stack.of(self).partition}:ecs:{Stack.of(self).region}:{Stack.of(self).account}:cluster/{config['ECS_CLUSTER_NAME']}"
                    }],
                    "status": ["ACTIVE"]
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

        # Optional EC2 Instance
        if config["CREATE_EC2_INSTANCE"]:
            # Look up VPC if provided
            if config["EC2_VPC_ID"]:
                vpc = ec2.Vpc.from_lookup(self, "ImportedVpc", vpc_id=config["EC2_VPC_ID"])
            else:
                # Create a default VPC if none provided
                vpc = ec2.Vpc(self, "DefaultVpc",
                    nat_gateways=1,
                    subnet_configuration=[
                        ec2.SubnetConfiguration(
                            name="public",
                            subnet_type=ec2.SubnetType.PUBLIC,
                            cidr_mask=24
                        ),
                        ec2.SubnetConfiguration(
                            name="private",
                            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                            cidr_mask=24
                        )
                    ]
                )

            # Look up subnet if provided, otherwise use the first public subnet
            if config["EC2_SUBNET_ID"]:
                subnet = ec2.Subnet.from_subnet_id(self, "ImportedSubnet", subnet_id=config["EC2_SUBNET_ID"])
                subnet_selection = ec2.SubnetSelection(subnets=[subnet])
            else:
                subnet_selection = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)

            # Create security group for EC2 instance
            ec2_security_group = ec2.SecurityGroup(
                self, "EC2SecurityGroup",
                vpc=vpc,
                description="Security group for GPU EC2 instance",
                allow_all_outbound=True
            )

            # Allow SSH access
            ec2_security_group.add_ingress_rule(
                ec2.Peer.any_ipv4(),
                ec2.Port.tcp(22),
                "Allow SSH access from anywhere"
            )

            # Use specified AMI or get the latest Amazon Linux 2 AMI
            if config["EC2_AMI_ID"]:
                machine_image = ec2.MachineImage.generic_linux({
                    self.region: config["EC2_AMI_ID"]
                })
            else:
                machine_image = ec2.MachineImage.latest_amazon_linux2(
                    cpu_type=ec2.AmazonLinuxCpuType.X86_64
                )

            # Create EC2 instance
            ec2_instance = ec2.Instance(
                self, "GpuInstance",
                vpc=vpc,
                instance_type=ec2.InstanceType(config["EC2_INSTANCE_TYPE"]),
                machine_image=machine_image,
                security_group=ec2_security_group,
                vpc_subnets=subnet_selection,
                key_name=config["EC2_SSH_KEY_NAME"] if config["EC2_SSH_KEY_NAME"] else None
            )

            # Output EC2 instance information
            CfnOutput(
                self, "EC2InstanceId",
                value=ec2_instance.instance_id,
                description="EC2 Instance ID"
            )

            CfnOutput(
                self, "EC2PublicIP",
                value=ec2_instance.instance_public_ip,
                description="EC2 Instance Public IP"
            )

        # Optional FSx Lustre File System
        if config["CREATE_FSX_LUSTRE"]:
            # Use EC2 VPC if FSX VPC not specified
            if config["FSX_VPC_ID"]:
                fsx_vpc = ec2.Vpc.from_lookup(self, "FsxVpc", vpc_id=config["FSX_VPC_ID"])
            elif config["CREATE_EC2_INSTANCE"] and config["EC2_VPC_ID"]:
                fsx_vpc = ec2.Vpc.from_lookup(self, "SharedVpc", vpc_id=config["EC2_VPC_ID"])
            elif config["CREATE_EC2_INSTANCE"]:
                # Use the VPC created for EC2
                fsx_vpc = vpc
            else:
                # Create a new VPC for FSx
                fsx_vpc = ec2.Vpc(self, "FsxDefaultVpc",
                    nat_gateways=1,
                    subnet_configuration=[
                        ec2.SubnetConfiguration(
                            name="public",
                            subnet_type=ec2.SubnetType.PUBLIC,
                            cidr_mask=24
                        ),
                        ec2.SubnetConfiguration(
                            name="private",
                            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                            cidr_mask=24
                        )
                    ]
                )

            # Look up subnet if provided, otherwise use the first private subnet
            if config["FSX_SUBNET_ID"]:
                fsx_subnet = ec2.Subnet.from_subnet_id(self, "FsxImportedSubnet", subnet_id=config["FSX_SUBNET_ID"])
                fsx_subnet_selection = ec2.SubnetSelection(subnets=[fsx_subnet])
            elif config["CREATE_EC2_INSTANCE"] and config["EC2_SUBNET_ID"]:
                fsx_subnet = ec2.Subnet.from_subnet_id(self, "SharedSubnet", subnet_id=config["EC2_SUBNET_ID"])
                fsx_subnet_selection = ec2.SubnetSelection(subnets=[fsx_subnet])
            else:
                fsx_subnet_selection = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)

            # Create security group for FSx Lustre
            fsx_security_group = ec2.SecurityGroup(
                self, "FsxSecurityGroup",
                vpc=fsx_vpc,
                description="Security group for FSx Lustre file system",
                allow_all_outbound=True
            )

            # Allow access from EC2 security group if EC2 instance is created
            if config["CREATE_EC2_INSTANCE"]:
                fsx_security_group.add_ingress_rule(
                    ec2_security_group,
                    ec2.Port.tcp(988),
                    "Allow access from EC2 instance"
                )
                fsx_security_group.add_ingress_rule(
                    ec2_security_group,
                    ec2.Port.tcp(1021),
                    "Allow access from EC2 instance"
                )
                fsx_security_group.add_ingress_rule(
                    ec2_security_group,
                    ec2.Port.tcp(1022),
                    "Allow access from EC2 instance"
                )
                fsx_security_group.add_ingress_rule(
                    ec2_security_group,
                    ec2.Port.tcp(1023),
                    "Allow access from EC2 instance"
                )
            else:
                # Allow access from within the VPC
                fsx_security_group.add_ingress_rule(
                    ec2.Peer.ipv4(fsx_vpc.vpc_cidr_block),
                    ec2.Port.tcp(988),
                    "Allow access from within VPC"
                )
                fsx_security_group.add_ingress_rule(
                    ec2.Peer.ipv4(fsx_vpc.vpc_cidr_block),
                    ec2.Port.tcp(1021),
                    "Allow access from within VPC"
                )
                fsx_security_group.add_ingress_rule(
                    ec2.Peer.ipv4(fsx_vpc.vpc_cidr_block),
                    ec2.Port.tcp(1022),
                    "Allow access from within VPC"
                )
                fsx_security_group.add_ingress_rule(
                    ec2.Peer.ipv4(fsx_vpc.vpc_cidr_block),
                    ec2.Port.tcp(1023),
                    "Allow access from within VPC"
                )

            # Create FSx Lustre file system
            fsx_lustre = fsx.LustreFileSystem(
                self, "FsxLustre",
                vpc=fsx_vpc,
                vpc_subnet=fsx_subnet_selection,
                security_group=fsx_security_group,
                storage_capacity_gib=config["FSX_STORAGE_CAPACITY_GB"],
                deployment_type=fsx.LustreDeploymentType(config["FSX_DEPLOYMENT_TYPE"]),
                per_unit_storage_throughput=config["FSX_PER_UNIT_STORAGE_THROUGHPUT"]
            )

            # Output FSx Lustre information
            CfnOutput(
                self, "FsxLustreFileSystemId",
                value=fsx_lustre.file_system_id,
                description="FSx Lustre File System ID"
            )

            CfnOutput(
                self, "FsxLustreDnsName",
                value=fsx_lustre.dns_name,
                description="FSx Lustre DNS Name"
            )

            CfnOutput(
                self, "FsxLustreMountName",
                value=fsx_lustre.mount_name,
                description="FSx Lustre Mount Name"
            )
