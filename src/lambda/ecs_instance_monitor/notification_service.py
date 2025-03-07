import boto3
import logging
from utils import error_handler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class NotificationService:
    """Service for SNS notifications"""

    def __init__(self, sns_topic_arn):
        """
        Initialize notification service.

        Args:
            sns_topic_arn (str): SNS topic ARN
        """
        self.sns_topic_arn = sns_topic_arn
        self.client = boto3.client('sns')

    @error_handler
    def send_notification(self, subject, message):
        """
        Send an SNS notification.

        Args:
            subject (str): Notification subject
            message (str): Notification message

        Returns:
            dict: Response from SNS publish API
        """
        logger.info(f"[NOTIFICATION_SEND] Sending notification: {subject}")
        response = self.client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"[NOTIFICATION_SENT] Notification sent: {subject}")
        return response
