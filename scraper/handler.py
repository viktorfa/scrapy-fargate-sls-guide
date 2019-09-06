import json
from datetime import datetime, timedelta
import logging

from launch_fargate import launch_fargate


def run_crawlers(event, context):
    result = []
    for x in filter(should_crawl, get_crawler_config()):
        try:
            if x.get("run_in_lambda"):
                # TODO Implement function to invoke crawl in new Lambda function
                pass
            else:
                fargate_launch_response = launch_fargate(dict(
                    spider_name=x.get("spider_name"),
                    spider_kwargs=x.get("spider_kwargs"),
                ), {})
                result.append(fargate_launch_response)
        except Exception:
            logging.exception(
                f"Could not launch spider {x.get('spider_name')}"
            )
    return result


def get_crawler_config():
    # This function could make an API call to a CMS like AirTable,
    # Strapi or DynamoDB.
    return [
        {
            "spider_name": "header_spider",
            "spider_kwargs": {
                "start_urls": ["https://example.com"],
            },
            "previous_crawl": {
                "success_state": True,
                "items_crawled": 100,
                "finish_date": datetime.now() - timedelta(days=3),
            },
        },
        {
            "spider_name": "header_spider",
            "spider_kwargs": {
                "start_urls": ["https://www.ietf.org"],
            },
            "previous_crawl": {
                "success_state": True,
                "items_crawled": 100,
                "finish_date": datetime.now() - timedelta(hours=16),
            },
            "crawl_interval_hours": 12,
            "settings": {
                "AUTOTHROTTLE_ENABLED": True,
            }
        },
        {
            "spider_name": "header_spider",
            "spider_kwargs": {
                "start_urls": ["https://bitcoin.org"],
            },
        },
        {
            "spider_name": "other_spider",
            "previous_crawl": None
        },
    ]


def should_crawl(x):
    previous_crawl = x.get("previous_crawl")
    if previous_crawl:
        time_interval_hours = x.get("crawl_interval_hours", 24*7)
        return previous_crawl.get("finish_date") + timedelta(hours=time_interval_hours) < datetime.now() or not previous_crawl.get("success_state")
    return True
