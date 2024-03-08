# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

from logging import PlaceHolder
import os
import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
    jsonify,
)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile
from helpers import from_timestamp_to_str, generate_presigned_url


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy fields/conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html",
        s3_post=presigned_post,
        is_premium_user=session["role"] == "premium_user",
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get("bucket"))
    s3_key = str(request.args.get("key"))

    # Extract the job ID from the S3 key
    if s3_key is None or bucket_name is None:
        return (
            jsonify(
                {
                    "code": 500,
                    "status": "error",
                    "message": "Key or bucket parameters are missing in the request.",
                }
            ),
            500,
        )

    # Persist job to database
    try:
        file_name_with_id = os.path.basename(s3_key)
        job_id = file_name_with_id.split("~")[0]
        file_name = "~".join(file_name_with_id.split("~")[1:])
        data = {
            "job_id": job_id,
            "user_id": session["primary_identity"],
            "user_email": session.get("email"),
            "file_name": file_name,  # without the UUID & delimiter
            "s3_input_bucket": bucket_name,
            "s3_key_input_file": s3_key,
            "submit_time": int(time.time()),
            "job_status": "PENDING",
        }
        dynamo = boto3.resource("dynamodb")
        table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        table.put_item(Item=data)

        # Send message to request queue
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn=app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
            Message=str(data),
        )
    except ClientError as e:
        app.logger.exception(f"An error occurred while uploading the job: {e}")

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    # keys: job_id, submit_time, input_file_name, job_status
    annotations: list[dict] | None = None
    # Get list of annotations to display
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
    response = table.query(
        IndexName="user_id_index",
        KeyConditionExpression=Key("user_id").eq(session["primary_identity"]),
    )
    items = response["Items"]
    annotations = []
    for item in items:
        annotations.append(
            {
                "job_id": item["job_id"],
                "submit_time": from_timestamp_to_str(int(item["submit_time"])),
                "input_file_name": item["file_name"],
                "job_status": item["job_status"],
            }
        )

    return render_template("annotations.html", annotations=annotations)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    # Keys: job_id, submit_time, input_file_name, input_file_url, job_status, complete_time, restore_message?, result_file_url?
    annotation = {}
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
    response = table.get_item(Key={"job_id": id})
    item = response["Item"]
    if item["user_id"] != session["primary_identity"]:
        return "", 405

    annotation["job_id"] = id
    annotation["submit_time"] = from_timestamp_to_str(int(item["submit_time"]))
    annotation["input_file_name"] = item["file_name"]
    annotation["input_file_url"] = generate_presigned_url(
        bucket_name=item["s3_input_bucket"], object_key=item["s3_key_input_file"]
    )
    annotation["job_status"] = item["job_status"]
    free_access_expired = False
    if annotation["job_status"] == "COMPLETED":
        annotation["complete_time"] = item["complete_time"]
        annotation["result_file_url"] = generate_presigned_url(
            bucket_name=item["s3_results_bucket"], object_key=item["s3_key_result_file"]
        )
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        complete_time = datetime.fromtimestamp(int(annotation["complete_time"]))
        if session.get("role") == "free_user" and complete_time < five_minutes_ago:
            free_access_expired = True

        # Check if the file is being restoring
        s3 = boto3.client("s3")
        response = s3.head_object(
            Bucket=item["s3_results_bucket"],
            Key=item["s3_key_result_file"],
        )

        if (
            "Restore" in response
            and 'ongoing-request="true"' in response["Restore"]
        ):
            annotation["restore_message"] = "File is being restoring, please come back later."

    return render_template(
        "annotation_details.html",
        annotation=annotation,
        free_access_expired=free_access_expired,
    )


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
    response = table.get_item(Key={"job_id": id})
    item = response["Item"]
    if item["user_id"] != session["primary_identity"]:
        return "", 405
    bucket_name = item["s3_results_bucket"]
    object_key = item["s3_key_log_file"]
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    log_file_contents = obj["Body"].read().decode("utf-8")
    return render_template(
        "view_log.html", job_id=id, log_file_contents=log_file_contents
    )


"""Subscription management handler
"""


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info
        if session.get("role") == "free_user":
            return render_template("subscribe.html")
        else:
            return redirect(url_for("profile"))

    else:
        # Update user role to allow access to paid features
        update_profile(identity_id=session["primary_identity"], role="premium_user")

        # Update role in the session
        session["role"] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!
        dynamo = boto3.resource("dynamodb")
        table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        response = table.query(
            IndexName="user_id_index",
            KeyConditionExpression=Key("user_id").eq(session["primary_identity"]),
        )
        items = response["Items"]
        s3 = boto3.client("s3")

        for item in items:
            response = s3.head_object(
                Bucket=item["s3_results_bucket"], Key=item["s3_key_result_file"]
            )
            storage_class = response.get("StorageClass")
            if storage_class == "GLACIER":
                app.logger.info(f"Push restore job for {item['job_id']} to sqs.")
                sqs = boto3.client("sqs", region_name=app.config["AWS_REGION_NAME"])
                queue_url = sqs.get_queue_url(QueueName=app.config["AWS_SQS_RESTORE_REQUEST_QUEUE_NAME"])[
                    "QueueUrl"
                ]
                body = {
                "job_id": item["job_id"],
                "s3_key_result_file": item["s3_key_result_file"],
                "s3_results_bucket": item["s3_results_bucket"],
                }
                response = sqs.send_message(
                    QueueUrl=queue_url, MessageBody=json.dumps(body)
                )
        # Display confirmation page
        return render_template("subscribe_confirm.html")


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


### EOF
