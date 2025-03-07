# ECS GPU 恢复系统部署所需权限

本文档列出了部署 ECS GPU 恢复系统所需的 AWS 权限。这些权限可以通过 IAM 策略附加到您的用户或角色。

## 完整权限（简化版）

如果您不需要遵循最小权限原则，可以使用以下简化的权限策略：

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudformation:*",
                "lambda:*",
                "dynamodb:*",
                "iam:*",
                "events:*",
                "sns:*",
                "ecs:*",
                "ssm:*",
                "logs:*",
                "s3:*"
            ],
            "Resource": "*"
        }
    ]
}
```

## 使用通配符的简化权限策略

如果您想要一个更简洁但仍然相对安全的策略，可以使用以下带有通配符的权限策略：

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudformation:*Stack*",
                "cloudformation:GetTemplate",
                "cloudformation:ValidateTemplate"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:*Function*",
                "lambda:*Permission*",
                "lambda:*Version*",
                "lambda:*Alias*",
                "lambda:GetPolicy"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:*Table*",
                "dynamodb:*TimeToLive*",
                "dynamodb:*Backup*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:*Role*",
                "iam:PassRole"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "events:*Rule*",
                "events:*Target*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sns:*Topic*",
                "sns:*Subscription*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ecs:Describe*",
                "ecs:List*",
                "ecs:RunTask",
                "ecs:StartTask",
                "ecs:StopTask",
                "ecs:*TaskDefinition*",
                "ecs:*Attributes*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:DescribeInstanceInformation",
                "ssm:*Command*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:*LogGroup*",
                "logs:*LogStream*",
                "logs:*LogEvents*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*Bucket*",
                "s3:*Object*"
            ],
            "Resource": [
                "arn:aws:s3:::cdktoolkit-stagingbucket-*",
                "arn:aws:s3:::cdktoolkit-stagingbucket-*/*"
            ]
        }
    ]
}
```

## 注意事项

1. 上述权限策略仅包含部署 ECS GPU 恢复系统所需的权限。根据您的具体环境和需求，可能需要调整这些权限。

2. 在生产环境中，建议遵循最小权限原则，仅授予必要的权限。

3. 如果您使用自定义资源名称（例如，自定义 DynamoDB 表名或 SNS 主题名），请相应地调整资源 ARN。

4. 如果您在特定区域部署，可以在资源 ARN 中指定区域，以进一步限制权限范围。
