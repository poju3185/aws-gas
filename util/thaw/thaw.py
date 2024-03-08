# thaw.py
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
import helpers

# Get configuration
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read("thaw_config.ini")

# Add utility code here
import boto3
from botocore.exceptions import ClientError

sqs = boto3.client("sqs", region_name=config["aws"]["AwsRegionName"])
queue_url = sqs.get_queue_url(QueueName=config["aws"]["AwsSqsRestorePendingQueueName"])[
    "QueueUrl"
]
s3 = boto3.client("s3")

print("Start listening to the restore pending queue...")
while True:
    try:
        # Attempt to read a message from the queue
        # Use long polling - DO NOT use sleep() to wait between polls
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,  # In production, this can be set longer
        )
        # If message read, extract job parameters from the message body as before

        if "Messages" in messages:
            # Get the first message
            for msg in messages["Messages"]:
                # Get the message body
                body = msg["Body"]
                body_json = json.loads(body)
                print(body_json)

                job_id = body_json["job_id"]
                bucket_name = body_json["s3_results_bucket"]
                object_key = body_json["s3_key_result_file"]
                response = s3.head_object(
                    Bucket=bucket_name,
                    Key=object_key,
                )

                if (
                    "Restore" in response
                    and 'ongoing-request="false"' in response["Restore"]
                ):
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={"Bucket": bucket_name, "Key": object_key},
                        Key=object_key,
                        StorageClass="STANDARD",
                    )
                    # Only delete the message when the file has been restored
                    print(
                        f"Job {job_id} has been restored and can be downloaded now!"
                    )
                    sqs.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
                else:
                    print(f"Job {job_id} is still being restoring, will come back and check later.")

    except KeyError as e:
        print(f"error {e}")
    except ClientError as e:
        print(f"error {e}")
### EOF
