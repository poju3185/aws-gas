# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

from math import log
import sys
import time
import driver
import boto3
import os
from configparser import ConfigParser
import json

config = ConfigParser(os.environ)
config.read("ann_config.ini")

"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == "__main__":
    # Call the AnnTools pipeline
    if len(sys.argv) > 4:
        input_file_path = sys.argv[1]
        user_id = sys.argv[2]
        user_email = sys.argv[3]
        job_id = sys.argv[4]
        input_file_name = os.path.basename(input_file_path)
        with Timer():
            driver.run(input_file_path, "vcf")

        data_folder_name = config["local"][
            "DataFolderName"
        ]  # The local dir name storing the results
        results_bucket_name = config["aws"]["AwsS3ResultsBucket"]
        aws_s3_key_prefix = config["aws"]["AwsS3KeyPrefix"]

        # Upload to s3
        s3 = boto3.client("s3", region_name=config["aws"]["AwsRegionName"])
        annot_file_name = input_file_name.split(".")[0] + ".annot.vcf"
        annot_file_key = f"{aws_s3_key_prefix}/{user_id}/{annot_file_name}"
        s3.upload_file(
            f"{data_folder_name}/{annot_file_name}",
            Bucket=results_bucket_name,
            Key=annot_file_key,
        )
        log_file_name = input_file_name + ".count.log"
        log_file_key = f"{aws_s3_key_prefix}/{user_id}/{log_file_name}"
        s3.upload_file(
            f"{data_folder_name}/{log_file_name}",
            Bucket=results_bucket_name,
            Key=log_file_key,
        )
        print(f"Result for {input_file_name} has been uploaded.")

        # Update dynamoDB
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(config["aws"]["AwsDynamodbAnnotationsTable"])
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="""
                SET job_status = :status, 
                    s3_results_bucket = :results_bucket,
                    s3_key_result_file = :result_key,
                    s3_key_log_file = :log_key,
                    complete_time = :complete_time
            """,
            ExpressionAttributeValues={
                ":status": "COMPLETED",
                ":results_bucket": config["aws"]["AwsS3ResultsBucket"],
                ":result_key": annot_file_key,
                ":log_key": log_file_key,
                ":complete_time": int(time.time()),
            },
        )

        message = {
            "email": user_email,
            "message": f"""Dear user:
            Your annotation job {job_id} is finished.""",
        }
        # Send message to result queue
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn=config["aws"]["AwsSnsJobCompleteTopic"],
            Message=json.dumps(message),
        )
        print(f"Sent notification for {input_file_name}.")

        sqs = boto3.client("sqs", region_name=config["aws"]["AwsRegionName"])
        queue_url = sqs.get_queue_url(QueueName=config["aws"]["AwsSqsArchiveRequestQueueName"])[
            "QueueUrl"
        ]
        body = {
            "job_id": job_id,
            "user_id": user_id,
            "s3_key_result_file": annot_file_key,
            "s3_results_bucket": results_bucket_name,
        }
        response = sqs.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(body)
        )
        print(f"Pushed delayed archive request for job {job_id}")

        # Deleted local file
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        os.remove(os.path.join(cur_dir, data_folder_name, annot_file_name))
        os.remove(os.path.join(cur_dir, data_folder_name, log_file_name))
        os.remove(os.path.join(cur_dir, input_file_path))

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF
