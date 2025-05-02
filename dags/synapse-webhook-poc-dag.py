"""
This DAG demonstrates polling an SQS queue and sending notifications to Synapse users.
1. Set up a sensor to poll an SQS queue
2. Process the messages from the queue
3. Send notifications to Synapse users about the received messages
"""

from datetime import datetime
from typing import List, Dict, Any, Union

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.sqs import SQSHook

from orca.services.synapse import SynapseHook

# Default parameters for the DAG
dag_params = {
    "aws_conn_id": Param(
        "AWS_DNT_DEV_SQS_CONN", type="string", description="AWS connection ID to use"
    ),
    "sqs_queue_url": Param(
        "https://sqs.us-east-1.amazonaws.com/631692904429/dev-synapse-sqs-create-queue",
        type="string",
        description="URL of the SQS queue to poll",
    ),
    "synapse_conn_id": Param(
        "SYNAPSE_ORCA_SERVICE_ACCOUNT_CONN",
        type="string",
        description="Synapse connection ID to use",
    ),
    "user_list": Param(
        "bwmac",
        type="string",
        description="Comma-separated list of Synapse users to notify",
    ),
    "message_subject": Param(
        "New SQS Message Notification",
        type="string",
        description="Subject of the Synapse notification",
    ),
    "max_messages": Param(
        10,
        type="integer",
        description="Maximum number of messages to retrieve per polling",
    ),
    "wait_time_seconds": Param(
        20,
        type="integer",
        description="The time in seconds to wait for receiving messages",
    ),
}


@dag(
    schedule_interval="*/15 * * * *",  # Run every 15 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["sqs", "synapse", "notifications"],
    params=dag_params,
)
def sqs_polling_synapse_notification_dag():
    """
    DAG that polls an SQS queue and sends notifications to Synapse users about received messages.
    """

    @task.sensor(poke_interval=60, timeout=300, mode="poke")
    def poll_sqs_queue(**context) -> List[Dict[str, Any]]:
        """
        Poll the SQS queue for messages.
        This task will return only when messages are found or when it times out.
        """
        sqs_hook = SQSHook(aws_conn_id=context["params"]["aws_conn_id"])
        queue_url = context["params"]["sqs_queue_url"]
        max_messages = context["params"]["max_messages"]
        wait_time_seconds = context["params"]["wait_time_seconds"]

        response = sqs_hook.get_conn().receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )

        if "Messages" in response and len(response["Messages"]) > 0:
            # Delete messages from the queue
            entries = [
                {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
                for msg in response["Messages"]
            ]

            sqs_hook.get_conn().delete_message_batch(
                QueueUrl=queue_url, Entries=entries
            )

            return response["Messages"]
        return []

    @task
    def process_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process the SQS messages before sending notifications.
        This could include data transformation, filtering, etc.
        """
        if not messages:
            return []

        processed_messages = []
        for message in messages:
            # Here you could transform or process the message as needed
            # For this example, we'll just pass the message body through
            processed_message = {
                "id": message["MessageId"],
                "body": message["Body"],
            }

            # Add any message attributes if they exist
            if "MessageAttributes" in message:
                processed_message["attributes"] = message["MessageAttributes"]

            processed_messages.append(processed_message)

        return processed_messages

    @task
    def send_synapse_notification(
        processed_messages: List[Dict[str, Any]], **context
    ) -> Union[int, None]:
        """
        Send notification to Synapse users about the received messages.
        """
        if not processed_messages:
            return

        user_list = context["params"]["user_list"].split(",")
        subject = context["params"]["message_subject"]

        # Construct message body from the processed messages
        message_body = "The following messages were received from the SQS queue:\n\n"
        for i, msg in enumerate(processed_messages, 1):
            message_body += f"Message {i}:\n"
            message_body += f"ID: {msg['id']}\n"
            message_body += f"Body: {msg['body']}\n\n"

        # Send notification to Synapse users
        hook = SynapseHook(context["params"]["synapse_conn_id"])
        id_list = [
            hook.client.getUserProfile(user).get("ownerId") for user in user_list
        ]

        hook.client.sendMessage(id_list, subject, message_body)

        return len(processed_messages)

    # Define task dependencies
    messages = poll_sqs_queue()
    processed_msgs = process_messages(messages)
    send_synapse_notification(processed_msgs)


# Instantiate the DAG
sqs_polling_synapse_notification_dag()
