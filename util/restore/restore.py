# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os
import sys
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))

# Get configuration
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read("restore_config.ini")

# Add utility code here
import boto3
from botocore.exceptions import ClientError

sqs = boto3.client("sqs", region_name=config["aws"]["AwsRegionName"])
queue_url = sqs.get_queue_url(QueueName=config["aws"]["AwsSqsRestoreRequestQueueName"])[
    "QueueUrl"
]
pending_queue_url = sqs.get_queue_url(
    QueueName=config["aws"]["AwsSqsRestorePendingQueueName"]
)["QueueUrl"]
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(config["aws"]["AwsDynamodbAnnotationsTable"])
s3 = boto3.client("s3")

print("Start listening to the restore request queue...")
while True:
    try:
        # Attempt to read a message from the queue
        # Use long polling - DO NOT use sleep() to wait between polls
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=3,
        )
        # If message read, extract job parameters from the message body as before

        if "Messages" in messages:
            # Get the first message
            msg = messages["Messages"][0]

            # Get the message body
            body = msg["Body"]
            body_json = json.loads(body)
            print(body_json)

            job_id = body_json["job_id"]
            bucket_name = body_json["s3_results_bucket"]
            object_key = body_json["s3_key_result_file"]
            try:
                response = s3.restore_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    RestoreRequest={
                        "Days": 1,  # The number of days the object remains accessible after recovery
                        "GlacierJobParameters": {
                            "Tier": "Expedited"  # Options: Standard, Bulk, Expedited
                        },
                    },
                )
            except s3.exceptions.ClientError as e:
                print("graceful degradation")
                response = s3.restore_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    RestoreRequest={
                        "Days": 1,  # The number of days the object remains accessible after recovery
                        "GlacierJobParameters": {
                            "Tier": "Standard"  # Options: Standard, Bulk, Expedited
                        },
                    },
                )
            # push a message to sqs
            body = {
                "job_id": job_id,
                "s3_key_result_file": object_key,
                "s3_results_bucket": bucket_name,
            }
            response = sqs.send_message(
                QueueUrl=pending_queue_url, MessageBody=json.dumps(body)
            )
            print(f"Start Restoring '{object_key}' in bucket '{bucket_name}'")

            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    except KeyError as e:
        print(f"error {e}")
    except ClientError as e:
        print(f"error {e}")

### EOF
