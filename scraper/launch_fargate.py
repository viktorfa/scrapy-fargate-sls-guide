import os
import json

import boto3


def launch_fargate(event, context):
    client = boto3.client("ecs")

    ECSCluster = os.environ["ECS_CLUSTER"]
    ECSSecGroup = os.environ["ECS_SEC_GROUP"]
    ECSSubnet = os.environ["ECS_SUBNET"]
    ECSTaskArn = os.environ["ECS_TASK_ARN"]
    CONTAINER_NAME = os.environ["CONTAINER_NAME"]

    run_task_response = client.run_task(
        cluster=ECSCluster,
        taskDefinition=ECSTaskArn,
        count=1,
        launchType="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": CONTAINER_NAME,
                    # We override the command so that we can pass some arguments
                    # e.g. for running different spiders.
                    "command": [
                        "python",
                        "launcher.py",
                        json.dumps(event),
                    ],
                    "environment": [
                        {"name": "FEED_BUCKET_NAME",
                            "value": os.environ["FEED_BUCKET_NAME"]},
                        {"name": "HTTP_CACHE_BUCKET_NAME",
                            "value": os.environ["HTTP_CACHE_BUCKET_NAME"]},
                    ],
                }
            ],
        },
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": [
                    ECSSubnet,
                ],
                "securityGroups": [
                    ECSSecGroup,
                ],
                "assignPublicIp": "ENABLED"
            },
        },
    )
    return json.dumps(run_task_response, default=str)
