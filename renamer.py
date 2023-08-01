#!/usr/bin/env python
# coding: utf-8

import concurrent.futures
import time
from typing import Dict
import argparse
import logging
import boto3
from pydantic_settings import BaseSettings, SettingsConfigDict
from botocore.config import Config

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--delete", action="store_true")
args = parser.parse_args()


class Settings(BaseSettings):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    SOURCE_BUCKET: str
    DEST_BUCKET: str
    DELETE_BUCKET: str
    REGION_NAME: str
    WORKERS: int = 16  # defaults to 16
    LOG_LEVEL: str = "INFO"  # defaults to INFO

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper()))
logger = logging.getLogger(__name__)

session = boto3.Session(
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.REGION_NAME,
)

# Don't allow more workers than connections
s3_config = Config(max_pool_connections=settings.WORKERS)

s3_client = boto3.client("s3", region_name=settings.REGION_NAME, config=s3_config)


def rename_file(
    source_bucket: str, dest_bucket: str, obj: Dict, counter: int, start_time
):
    old_key = obj["Key"]
    # logger.info(f"old_key: {old_key}")
    if "locationid" in old_key:
        new_key = "records/csv.gz/" + old_key[old_key.index("locationid") :]
        # logger.info(f"new key: {new_key}")
        s3_client.copy_object(
            Bucket=dest_bucket,
            CopySource={"Bucket": source_bucket, "Key": old_key},
            Key=new_key,
        )
        counter[0] += 1
        elapsed_time = time.time() - start_time
        if counter[0] % 1000 == 0:
            logger.info(
                f"\nRenamed files: {counter[0]}, Elapsed time: {round(elapsed_time)} seconds"
            )


def rename_files(source_bucket: str, dest_bucket: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    counter = [0]  # Using list so that it is mutable inside rename_file
    start_time = time.time()
    for page in paginator.paginate(Bucket=source_bucket):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    rename_file, source_bucket, dest_bucket, obj, counter, start_time
                ): obj
                for obj in page["Contents"]
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(
                        "%r generated an exception: %s" % (futures[future], exc)
                    )


def delete_files(bucket_name: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    counter = [0]  # Using a list so that it is mutable inside delete_file
    start_time = time.time()
    for page in paginator.paginate(Bucket=bucket_name):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.WORKERS
        ) as executor:
            futures = {}
            for obj in page["Contents"]:
                should_delete = not obj["Key"].startswith("records/csv.gz/locationid=")
                futures[
                    executor.submit(
                        delete_file,
                        bucket_name,
                        obj,
                        counter,
                        start_time,
                        should_delete,
                    )
                ] = obj

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(
                        "%r generated an exception: %s" % (futures[future], exc)
                    )


def delete_file(
    bucket_name: str, obj: Dict, counter: int, start_time, should_delete: bool
):
    key = obj["Key"]
    if should_delete:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        action = "Deleted"
        counter[0] += 1
    else:
        action = "Didn't delete"

    elapsed_time = time.time() - start_time
    if counter[0] % 1000 == 0:
        logger.info(
            f"\n{action}: {obj['Key']} \nDeleted files: {counter[0]} \nElapsed time: {round(elapsed_time)} seconds"
        )


# Use the bucket name from the .env file
if __name__ == "__main__":
    if args.delete:
        delete_files(settings.DELETE_BUCKET)  # Adjust as needed
    else:
        rename_files(settings.SOURCE_BUCKET, settings.DEST_BUCKET)  # Adjust as needed
