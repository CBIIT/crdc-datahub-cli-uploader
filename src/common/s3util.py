#!/usr/bin/env python
import boto3
from botocore.exceptions import ClientError

from bento.common.utils import get_logger
from common.constants import ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN

BUCKET_OWNER_ACL = 'bucket-owner-full-control'
SINGLE_PUT_LIMIT = 4_500_000_000

class S3Bucket:
    def __init__(self):
        self.log = get_logger('S3 Bucket')

    def set_s3_client(self, bucket, creadentials):
        self.bucket_name = bucket
        self.credential = creadentials
        session = boto3.session.Session(
            aws_access_key_id=creadentials[ACCESS_KEY_ID],
            aws_secret_access_key=creadentials[SECRET_KEY],
            aws_session_token=creadentials[SESSION_TOKEN]
        )
        self.client = session.client('s3')
        self.s3 = session.resource('s3')
        self.bucket = self.s3.Bucket(bucket)

    def put_file_obj(self, key, data, md5_base64):
        return self.bucket.put_object(Key=key,
                                      Body=data,
                                      ContentMD5=md5_base64,
                                      ACL= BUCKET_OWNER_ACL)

    def upload_file_obj(self, key, data, config=None, extra_args={'ACL': BUCKET_OWNER_ACL}):
        return self.bucket.upload_fileobj(data, key, ExtraArgs=extra_args, Config=config)

    def get_object_size(self, key):
        try:
            res = self.client.head_object(Bucket=self.bucket_name, Key=key)
            return res['ContentLength']

        except ClientError as e:
            return None

    def same_size_file_exists(self, key, file_size):
        return file_size == self.get_object_size(key)

