#!/usr/bin/env python
import os
import boto3
# from boto3.s3.transfer import TransferConfig, S3Transfer

from botocore.exceptions import ClientError
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from bento.common.utils import get_logger
from common.constants import ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN
from common.progress_bar import ProgressPercentage, create_progress_bar

BUCKET_OWNER_ACL = 'bucket-owner-full-control'
SINGLE_PUT_LIMIT = 4_500_000_000


class ProgressFileWrapper:
    def __init__(self, file_obj, progress, task_id):
        self._file = file_obj
        self._progress = progress
        self._task_id = task_id
        self._read_bytes = 0

    def read(self, size):
        data = self._file.read(size)
        if data:
            self._read_bytes += len(data)
            self._progress.update(self._task_id, completed=self._read_bytes)
        return data

    def __getattr__(self, name):
        return getattr(self._file, name)

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
            return True, None
        except ClientError as e:
            msg = None
            if e.response['Error']['Code'] in ['404', '412']:
                msg = f'File {key} does not exist in the specified S3 bucket path.'
                return False, msg
            if e.response['Error']['Code'] in ['403']:
                msg = f'Access Denied: Unable to access files in the specified S3 bucket path: {key}'
                return False, msg
            else:
                msg = f'Unknown S3 client error!'
                self.log.exception(e)
                return False, msg  

    def put_file_obj(self, file_size, key, data, md5_base64):
        # Initialize the progress bar
        progress = create_progress_bar(file_size)
        task = progress.add_task("uploading task", total=file_size)
        chunk_size = 1024 * 1024 if file_size >= 1024 * 1024 else file_size #chunk data for display progress for small metadata file < 4,500,000,000 bytes

        uploaded_bytes = 0
        with progress:
            while uploaded_bytes < file_size:
                chunk = data.read(chunk_size)
                if not chunk:
                    break  # Stop if there’s nothing left to read

                self.bucket.put_object(
                    Key=key,
                    Body=chunk,
                    ContentMD5=md5_base64,
                    ACL=BUCKET_OWNER_ACL,
                )
                uploaded_bytes += len(chunk)  # Track uploaded bytes
                progress.update(task, advance=len(chunk))



    def upload_file_obj(self, stream, key, progress_callback, config=None, extra_args={'ACL': BUCKET_OWNER_ACL}):
        print("tesetsetset....")
        self.bucket.upload_fileobj(
            stream, key, ExtraArgs=extra_args, Config=config, Callback=progress_callback)

    def get_object_size(self, key):
        try:
            res = self.client.head_object(Bucket=self.bucket_name, Key=key)
            return res['ContentLength'], None
        except ClientError as e:
            msg = None
            if e.response['Error']['Code'] in ['404', '412']:
                msg = f'File {key} does not exist in the specified S3 bucket path.'
                return None, msg
            if e.response['Error']['Code'] in ['403']:
                msg = f'Access Denied: Unable to access files in the specified S3 bucket path: {key}'
                return None, msg
            else:
                msg = f'Unknown S3 client error!'
                self.log.exception(e)
                return None, msg  

    def same_size_file_exists(self, key, file_size):
        file_size1, msg = self.get_object_size(key)
        if msg:
            # self.log.error(msg)
            return False
        return file_size == file_size1
    
    def download_object(self, key, local_file_path):
        try:
            file_size, msg = self.get_object_size(key)
            self.bucket.download_file(key, local_file_path,
                                      Callback=ProgressPercentage(file_size))
            return True, None
        except ClientError as ce:
            msg = None
            if e.response['Error']['Code'] in ['404', '412']:
                msg = f'File {key} does not exist in the specified S3 bucket path.'
                return False, msg
            if e.response['Error']['Code'] in ['403']:
                msg = f'Access Denied: Unable to access files in the specified S3 bucket path: {key}'
                return False, msg
            else:
                msg = f'Unknown S3 client error!'
                self.log.exception(e)
                return False, msg  
        except Exception as e:
            msg = f'Unknown error!'
            self.log.error(e)
            return False, msg
        
    def close(self):
        self.client.close()
        self.client = None
        self.bucket = None
        self.s3 = None

        

