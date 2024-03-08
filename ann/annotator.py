import os
import shutil
import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from uuid import uuid4
import subprocess
from subprocess import SubprocessError
from datetime import datetime, timedelta
import os
import logging
import json
import ast
from configparser import ConfigParser


current_file_path = os.path.abspath(__file__)
current_dir_path = os.path.dirname(current_file_path)
log_file_path = os.path.join(current_dir_path, "error.log")
config_file_path = os.path.join(current_dir_path, "ann_config.ini")
config = ConfigParser(os.environ)
config.read(config_file_path)
logging.basicConfig(filename=log_file_path, level=logging.ERROR)


region = config["aws"]["AwsRegionName"]
s3 = boto3.resource("s3", region_name=region)
s3_client = boto3.client("s3", region_name=region)
results_bucket = s3.Bucket(config["aws"]["AwsS3ResultsBucket"])
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(config["aws"]["AwsDynamodbAnnotationsTable"])
# Connect to SQS and get the message queue
sqs = boto3.client("sqs", region_name=region)
queue_url = sqs.get_queue_url(QueueName=config["aws"]["AwsSqsJobRequestQueueName"])[
    "QueueUrl"
]

# Poll the message queue in a loop
print("Start listening to the message queue...")
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
            data = ast.literal_eval(body_json["Message"])

            bucket = data["s3_input_bucket"]
            key = data["s3_key_input_file"]
            user_id = data["user_id"]
            user_email = data["user_email"]
            job_id = data["job_id"]
            file_name = data["file_name"]
            print(f"Processing job {key}..")

            # Include below the same code you used in prior homework
            # Get the input file S3 object and copy it to a local file
            # Use a local directory structure that makes it easy to organize
            # multiple running annotation jobs

            if not key.endswith(".vcf"):
                s3_client.delete_object(Bucket=bucket, Key=key)

            file_path = os.path.join(current_dir_path, "data", f"{job_id}~{file_name}")
            os.makedirs(
                os.path.dirname(file_path), exist_ok=True
            )  # Create dir if doesn't exist
            with open(file_path, "wb") as f:
                s3_client.download_fileobj(bucket, key, f)

            run_file_path = os.path.join(current_dir_path, "run.py")
            ps = subprocess.Popen(
                ["python3", run_file_path, file_path, user_id, user_email, job_id]
            )
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :new_status",
                ConditionExpression="job_status = :expected_status",
                ExpressionAttributeValues={
                    ":new_status": "RUNNING",
                    ":expected_status": "PENDING",
                },
            )

            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    except KeyError as e:
        print(f"error {e}")
        logging.error(f"Key error {e} while handling message: {messages}")
    except SubprocessError as e:
        print(f"error {e}")
        logging.error(f"SubprocessError {e} while handling message: {messages}")
    except ClientError as e:
        print(f"error {e}")
        logging.error(f"ClientError {e} while handling message: {messages}")
