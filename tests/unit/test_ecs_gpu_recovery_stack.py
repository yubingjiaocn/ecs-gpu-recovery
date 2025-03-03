import aws_cdk as core
import aws_cdk.assertions as assertions

from ecs_gpu_recovery.ecs_gpu_recovery_stack import EcsGpuRecoveryStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ecs_gpu_recovery/ecs_gpu_recovery_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EcsGpuRecoveryStack(app, "ecs-gpu-recovery")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
