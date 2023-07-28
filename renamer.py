#!/usr/bin/env python
# coding: utf-8

import boto3
import concurrent.futures
import time

# Manually specify AWS credentials (!)

aws_access_key_id = "your-access-key-here"
aws_secret_access_key = "your-secret-key-here"

region_name = "us-east-1"

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",  # OpenAQ data is stored in the 'us-east-1' region
)

s3_client = boto3.client("s3", region_name="us-east-1")


def rename_file(bucket_name, obj, counter, start_time):
    old_key = obj["Key"]
    if "locationid" in old_key:
        new_key = "records/csv.gz/" + old_key[old_key.index("locationid") :]
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": old_key},
            Key=new_key,
        )
        #         s3_client.delete_object(Bucket=bucket_name, Key=old_key) # delete makes it take longer
        counter[0] += 1
        elapsed_time = time.time() - start_time
        print(f"\nRenamed files: {counter[0]}, Elapsed time: {elapsed_time} seconds")


def rename_files(bucket_name):
    paginator = s3_client.get_paginator("list_objects_v2")
    counter = [0]  # Using list so that it is mutable inside rename_file
    start_time = time.time()
    for page in paginator.paginate(Bucket=bucket_name):
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = {
                executor.submit(rename_file, bucket_name, obj, counter, start_time): obj
                for obj in page["Contents"]
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print("%r generated an exception: %s" % (futures[future], exc))


## Use the right bucket!
bucket_name = "paste-the-correct-bucket-name-here"
rename_files(bucket_name)
