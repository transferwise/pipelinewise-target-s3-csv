#!/usr/bin/env python3

import os
import backoff
import boto3
from botocore.client import Config
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session

LOGGER = singer.get_logger()


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def setup_aws_client(config):
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']

    LOGGER.info("Attempting to create AWS session")
    boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

@retry_pattern()
def upload_file(filename, bucket, key_prefix,
                encryption_type=None, encryption_key=None):
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
    s3_key = "{}{}".format(key_prefix, os.path.basename(filename))

    if encryption_type is None or encryption_type.lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ""
        encryption_args = None
    else:
        if encryption_type.lower() == "kms":
            encryption_args = {"ServerSideEncryption": "aws:kms"}
            if encryption_key:
                encryption_desc = (
                    " using KMS encryption key ID '{}'"
                    .format(encryption_key)
                )
                encryption_args["SSEKMSKeyId"] = encryption_key
            else:
                encryption_desc = " using default KMS encryption"
        else:
            raise NotImplementedError(
                "Encryption type '{}' is not supported. "
                "Expected: 'none' or 'KMS'"
                .format(encryption_type)
            )
    LOGGER.info(
        "Uploading {} to bucket {} at {}{}"
        .format(filename, bucket, s3_key, encryption_desc)
    )
    s3_client.upload_file(filename, bucket, s3_key, ExtraArgs=encryption_args)
