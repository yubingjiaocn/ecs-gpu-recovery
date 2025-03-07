# ECS GPU 恢复系统部署指南

本文档提供了 ECS GPU 恢复系统的详细部署步骤，从安装 AWS CDK CLI 到完成部署的全过程。

## 目录

- [系统概述](#系统概述)
- [前提条件](#前提条件)
- [安装 AWS CDK CLI](#安装-aws-cdk-cli)
- [设置项目](#设置项目)
- [配置部署](#配置部署)
- [部署解决方案](#部署解决方案)
- [验证部署](#验证部署)
- [部署所需权限](#部署所需权限)

## 系统概述

ECS GPU 恢复系统是一个自动化解决方案，用于监控和恢复 Amazon ECS 集群中的 GPU 资源。当系统检测到 GPU 加速任务失败时，会运行诊断以确定故障是否与 GPU 相关，并采取适当的恢复措施，包括实例重启和任务重新启动。

### 主要功能

- 自动检测 GPU 任务故障
- 使用 NVIDIA DCGM 进行 GPU 健康检查
- 通过实例重启进行自我修复
- 恢复后自动重启任务
- 不可恢复故障的通知系统
- 重试限制，防止无限恢复循环

### 架构组件

- **Lambda 函数**
  - **ECS Task Handler**：监控任务故障并触发健康检查
  - **DCGM Task Monitor**：分析 GPU 健康检查结果
  - **ECS Instance Monitor**：处理实例恢复和任务重启

- **DynamoDB 表**
  - **ecs_task**：跟踪单个任务状态
  - **ecs_job**：管理作业状态（相关任务的集合）
  - **ecs_node**：监控容器实例健康状况

- **事件触发器**
  - 用于任务状态变化的 EventBridge 规则
  - 用于容器实例状态变化的 EventBridge 规则

## 前提条件

在开始部署之前，请确保满足以下条件：

1. **AWS 账户**：拥有有效的 AWS 账户，并具有创建和管理资源的权限
2. **AWS CLI**：已安装并配置 AWS CLI，具有适当的权限
3. **Python**：已安装 Python 3.11 或更高版本
4. **Node.js**：已安装 Node.js 14.x 或更高版本（CDK 的依赖）
5. **开发环境**：具有终端访问权限的开发环境

## 安装 AWS CDK CLI

AWS CDK (Cloud Development Kit) 是一个开发框架，允许使用熟悉的编程语言定义云基础设施。以下是安装 CDK CLI 的步骤：

### 步骤 1：安装 Node.js（如果尚未安装）

```bash
# 对于 Ubuntu/Debian
sudo apt update
sudo apt install -y nodejs npm

# 对于 Amazon Linux/RHEL/CentOS
sudo yum install -y nodejs npm

# 对于 macOS（使用 Homebrew）
brew install node

# 对于 Windows
# 从 https://nodejs.org 下载并安装 Node.js
```

### 步骤 2：安装 AWS CDK CLI

```bash
# 全局安装 CDK CLI
npm install -g aws-cdk
```

### 步骤 3：验证安装

```bash
# 检查 CDK 版本
cdk --version
```

确保 CDK 版本为 2.180.0 或更高版本。

## 设置项目

### 步骤 1：克隆代码库

```bash
# 克隆代码库
git clone https://github.com/yourusername/ecs-gpu-recovery.git
cd ecs-gpu-recovery
```

### 步骤 2：创建并激活虚拟环境

```bash
# 创建虚拟环境
python -m venv .venv

# 在 Linux/macOS 上激活虚拟环境
source .venv/bin/activate

# 在 Windows 上激活虚拟环境
.venv\Scripts\activate.bat
```

### 步骤 3：安装依赖

```bash
# 安装项目依赖
pip install -r requirements.txt
```

### 步骤 4：引导 AWS 环境（首次使用 CDK）

如果这是您第一次在 AWS 账户/区域中使用 CDK，需要执行引导过程：

```bash
# 使用默认配置文件引导环境
cdk bootstrap

# 或指定特定账户和区域
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

## 配置部署

在部署解决方案之前，您可能需要根据自己的环境调整配置。

### 方法 1：使用环境变量

您可以通过设置环境变量来覆盖默认配置：

```bash
# 设置 DynamoDB 表名
export TASK_TABLE_NAME="my_ecs_task"
export JOB_TABLE_NAME="my_ecs_job"
export NODE_TABLE_NAME="my_ecs_node"

# 设置 ECS 集群名称
export ECS_CLUSTER_NAME="my-gpu-cluster"

# 设置 DCGM 健康检查任务定义
export DCGM_HEALTH_CHECK_TASK="arn:aws:ecs:region:account:task-definition/gpu-dcgm-health-check:1"

# 设置 Lambda 配置
export LAMBDA_TIMEOUT_SECONDS="120"
export LAMBDA_MEMORY_SIZE="512"

# 设置 SNS 配置
export SNS_TOPIC_NAME="my-gpu-notifications"
export SNS_TOPIC_DISPLAY_NAME="My GPU Training Job Notifications"
```

### 方法 2：修改配置文件

或者，您可以直接修改 `ecs_gpu_recovery/config.py` 文件中的默认值：

```python
# 编辑配置文件
class Config:
    # DynamoDB Tables
    TASK_TABLE_NAME = "my_ecs_task"
    JOB_TABLE_NAME = "my_ecs_job"
    NODE_TABLE_NAME = "my_ecs_node"

    # ECS Configuration
    ECS_CLUSTER_NAME = "my-gpu-cluster"
    DCGM_HEALTH_CHECK_TASK = "arn:aws:ecs:region:account:task-definition/gpu-dcgm-health-check:1"

    # 其他配置...
```

## 部署解决方案

### 步骤 1：合成 CloudFormation 模板

```bash
# 合成 CloudFormation 模板
cdk synth
```

这将生成 CloudFormation 模板，您可以在 `cdk.out` 目录中查看。

### 步骤 2：部署堆栈

```bash
# 部署堆栈
cdk deploy
```

在部署过程中，CDK 将显示将要创建的资源列表，并要求您确认。输入 `y` 确认部署。

如果您想跳过确认步骤，可以使用 `--require-approval never` 选项：

```bash
cdk deploy --require-approval never
```

### 步骤 3：查看部署输出

部署完成后，CDK 将显示输出信息，包括创建的资源的 ARN 和其他重要信息。

## 验证部署

部署完成后，您可以通过以下步骤验证部署是否成功：

### 步骤 1：检查 CloudFormation 堆栈

```bash
# 列出 CloudFormation 堆栈
aws cloudformation describe-stacks --stack-name EcsGpuRecoveryStack
```

确保堆栈状态为 `CREATE_COMPLETE`。

### 步骤 2：检查 DynamoDB 表

```bash
# 列出 DynamoDB 表
aws dynamodb list-tables
```

确认已创建 `ecs_task`、`ecs_job` 和 `ecs_node` 表（或您自定义的表名）。

### 步骤 3：检查 Lambda 函数

```bash
# 列出 Lambda 函数
aws lambda list-functions
```

确认已创建 `EcsTaskHandler`、`DcgmTaskMonitor` 和 `EcsInstanceMonitor` 函数。

### 步骤 4：检查 EventBridge 规则

```bash
# 列出 EventBridge 规则
aws events list-rules
```

确认已创建用于监控 ECS 任务和实例状态变化的规则。

### 步骤 5：检查 SNS 主题

```bash
# 列出 SNS 主题
aws sns list-topics
```

确认已创建通知主题。

## 部署所需权限

要成功部署此解决方案，您需要一定的 AWS 权限。请参考[此文档](./ECS_GPU_Recovery_Permissions_CN.md)以获取权限列表。

## 故障排除

如果在部署过程中遇到问题，请尝试以下步骤：

1. **检查 CDK 版本**：确保您使用的是 CDK 2.180.0 或更高版本
2. **检查 AWS 凭证**：确保您的 AWS 凭证有效且具有足够的权限
3. **检查日志**：查看 CloudFormation 和 Lambda 日志以获取错误详情
4. **清理资源**：如果部署失败，使用 `cdk destroy` 清理已创建的资源，然后重试

如果问题仍然存在，请查看 AWS CDK 文档或联系 AWS 支持。
