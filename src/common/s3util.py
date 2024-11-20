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

    def set_s3_client(self, bucket, credentials):
        self.bucket_name = bucket
        
        if credentials:
            self.credential = credentials
            session = boto3.session.Session(
                aws_access_key_id=credentials[ACCESS_KEY_ID],
                aws_secret_access_key=credentials[SECRET_KEY],
                aws_session_token=credentials[SESSION_TOKEN]
            )
            self.client = session.client('s3')
            self.s3 = session.resource('s3')
            self.bucket = self.s3.Bucket(bucket)
            
        else:
            self.client = boto3.client('s3')
            self.s3 = boto3.resource('s3')
            self.bucket = self.s3.Bucket(bucket)
            self.credential = None
        
    def file_exists_on_s3(self, key):
        '''
        Check if file exists in S3, return True only if file exists

        :param key: file path
        :return: boolean
        '''
        try:
            self.client.head_object(Bucket=self.bucket.name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ['404', '412']:
                return False
            if e.response['Error']['Code'] in ['403']:
                self.log.error('Access Denied: Unable to access files in the specified S3 bucket path.')
                return False
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)
                return False   

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
            if e.response['Error']['Code'] in ['404', '412']:
                return None
            if e.response['Error']['Code'] in ['403']:
                self.log.error('Access Denied: Unable to access files in the specified S3 bucket path.')
                return None
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)
                return None   

    def same_size_file_exists(self, key, file_size):
        return file_size == self.get_object_size(key)
    
    def download_object(self, key, local_file_path):
        try:
            self.bucket.download_file( key, local_file_path)
            return True
        except ClientError as ce:
            if e.response['Error']['Code'] in ['404', '412']:
                return False
            if e.response['Error']['Code'] in ['403']:
                self.log.error('Access Denied: Unable to access files in the specified S3 bucket path.')
                return False
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)
                return False   
        except Exception as e:
            self.log.error(e)
            return False
        
    def close(self):
        self.client.close()
        self.client = None
        self.bucket = None
        self.s3 = None
        

