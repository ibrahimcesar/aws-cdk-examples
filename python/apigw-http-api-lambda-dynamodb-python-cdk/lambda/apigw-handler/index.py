# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all AWS SDK calls for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    request_id = context.request_id
    
    # Structured logging with context
    logger.info(json.dumps({
        "message": "Processing request",
        "request_id": request_id,
        "table_name": table,
        "event_type": event.get("requestContext", {}).get("requestId"),
        "http_method": event.get("requestContext", {}).get("httpMethod"),
        "source_ip": event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
    }))
    
    if event["body"]:
        item = json.loads(event["body"])
        logger.info(json.dumps({
            "message": "Received payload",
            "request_id": request_id,
            "payload": item,
        }))
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        logger.info(json.dumps({
            "message": "Data inserted successfully",
            "request_id": request_id,
            "item_id": id,
        }))
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        logger.info(json.dumps({
            "message": "Request without payload, using default data",
            "request_id": request_id,
        }))
        default_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": default_id},
            },
        )
        message = "Successfully inserted data!"
        logger.info(json.dumps({
            "message": "Default data inserted successfully",
            "request_id": request_id,
            "item_id": default_id,
        }))
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
