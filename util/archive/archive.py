# archive.py
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
from helpers import get_user_profile

# Get configuration
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read("archive_config.ini")

import boto3
from botocore.exceptions import ClientError

# Connect to SQS and get the message queue
sqs = boto3.client("sqs", region_name=config["aws"]["AwsRegionName"])
queue_url = sqs.get_queue_url(QueueName=config["aws"]["AwsSqsArchiveRequestQueueName"])[
    "QueueUrl"
]
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(config["aws"]["AwsDynamodbAnnotationsTable"])
s3 = boto3.client("s3")
# Add utility code here
# Poll the message queue in a loop
print("Start listening to the archive request queue...")
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
            user_id = body_json["user_id"]
            bucket_name = body_json["s3_results_bucket"]
            object_key = body_json["s3_key_result_file"]
            profile = get_user_profile(id=user_id)
            if profile["role"] == "free_user":
                response = s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={"Bucket": bucket_name, "Key": object_key},
                    Key=object_key,
                    StorageClass="GLACIER",
                )
                print(f"Archived '{object_key}' in '{bucket_name}' to Glacier.")
            else:
                print(f"User '{user_id}' is a premium user, don't archive.")

            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    except KeyError as e:
        print(f"error {e}")
    except ClientError as e:
        print(f"error {e}")

### EOF
