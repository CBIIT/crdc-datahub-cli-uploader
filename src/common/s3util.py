#!/usr/bin/env python
import os
import math
import boto3
import datetime
from typing import BinaryIO, List
import time

from botocore.exceptions import ClientError

from bento.common.utils import get_logger
from common.constants import ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN, TEMP_CREDENTIAL, TEMP_TOKEN_EXPIRATION
from common.progress_bar import create_progress_bar, ProgressCallback
from common.graphql_client import APIInvoker
from common.utils import convert_string_to_date_time

BUCKET_OWNER_ACL = 'bucket-owner-full-control'
SINGLE_PUT_LIMIT = 5 * 1024 * 1024 * 1024  # 5GB
MAX_PART_NUMBER = 9999

class S3Bucket:
    def __init__(self):
        self.log = get_logger('S3 Bucket')
        self.parts: List[dict] = []
        self.configs = None
        self.expiration = None

    def set_s3_client(self, bucket, configs):
        self.bucket_name = bucket
        self.configs = configs
        credentials = configs.get(TEMP_CREDENTIAL) if configs else None
        if credentials:           
            self.credential = credentials
            session = boto3.session.Session(
                aws_access_key_id=credentials[ACCESS_KEY_ID],
                aws_secret_access_key=credentials[SECRET_KEY],
                aws_session_token=credentials[SESSION_TOKEN]
            )
            self.expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3600) \
                if not credentials.get(TEMP_TOKEN_EXPIRATION) else convert_string_to_date_time(credentials[TEMP_TOKEN_EXPIRATION])
            self.client = session.client('s3')
            self.s3 = session.resource('s3')
            self.bucket = self.s3.Bucket(bucket)
        else:
            self.client = boto3.client('s3')
            self.s3 = boto3.resource('s3')
            self.bucket = self.s3.Bucket(bucket)
            self.credential = None
    
    def refreshToken(self):
        apiInvoker = APIInvoker(self.configs)
        if apiInvoker.get_temp_credential(True):
            temp_credential = apiInvoker.cred
            self.configs[TEMP_CREDENTIAL] = temp_credential
            self.set_s3_client(self.bucket_name, self.configs)
            return True
        else:
            self.log.error("Failed to upload files: can't refresh temp credential!")
            return False   
    
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
        progress = create_progress_bar()
        task = progress.add_task("uploading task", total=file_size)
        chunk_size = 1024 * 1024 if file_size >= 1024 * 1024 else file_size #chunk data for display progress for small metadata file < 4,500,000,000 bytes

        uploaded_bytes = 0

        try:
            with progress:
                while uploaded_bytes < file_size:
                    chunk = data.read(chunk_size)
                    if not chunk:
                        break  # Stop if there’s nothing left to read

                    self.bucket.put_object(
                        Key=key,
                        Body=chunk,
                        ACL=BUCKET_OWNER_ACL,
                    )
                    uploaded_bytes += len(chunk)  # Track uploaded bytes
                    progress.update(task, advance=len(chunk))
        finally:
            progress.stop()

    def upload_file_obj(self, stream, key, progress_callback, file_name, config=None, extra_args={'ACL': BUCKET_OWNER_ACL}):
        if self.is_token_expired():
            self.refreshToken()
        extra_args.update({'ContentDisposition': f'attachment; filename="{file_name}"'})
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
            with create_progress_bar() as progress:
                file_size, msg = self.get_object_size(key)
                task_id = progress.add_task("Downloading object...", total=file_size)
                progress_callback = ProgressCallback(file_size, progress, task_id)
                self.bucket.download_file(key, local_file_path,
                                          Callback=progress_callback)
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
        
    # get contents info from s3 folder
    def get_contents(self, prefix):
        contents = []
        if not prefix.endswith('/'):
            prefix += '/'
        try:
            for obj in self.bucket.objects.filter(Prefix=prefix):
                # key end with ".tsv" or ".txt"
                base, ext = os.path.splitext(obj['Key'])
                if ext in [".tsv", ".txt"]:
                    contents.append(obj['Key'])    
        except Exception:
            self.log.error("Failed to retrieve child metadata files.")
        finally:
            return contents
        
    def get_contents_in_current_folder(self, prefix):
        contents = []
        if not prefix.endswith('/'):
            prefix += '/'
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/' ):
                for obj in page.get('Contents', []):
                    # key end with ".tsv" or ".txt"
                    base, ext = os.path.splitext(obj['Key'])
                    if ext in [".tsv", ".txt"]:
                        contents.append(obj['Key'])
        except Exception:
            self.log.error("Failed to retrieve child metadata files.")
        finally:
            return contents
    
    def put_file(self, s3_key, file_path):
        try:
            self.client.upload_file(file_path, self.bucket_name, s3_key)
        except Exception as e:
            self.log.error("Failed to upload file.")

    # check if token is expired with buffer seconds 
    def is_token_expired(self, buffer_seconds=300):
        expiration = self.expiration
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=datetime.timezone.utc)
        return datetime.datetime.now(datetime.timezone.utc) > (expiration - datetime.timedelta(seconds=buffer_seconds))

    # start manual multipart upload section
    # Upload a large file (size > 5 GB) in parts
    def upload_large_file_partly(self, fileobj: BinaryIO, key, size, progress_callback):
        self.parts = []
        part_size = self.calculate_part_size(size)
        try:
            self.initiate_multipart_upload(key)
            total_parts = math.ceil(size / part_size)
            for part_number in range(1, total_parts + 1):
                data = fileobj.read(part_size)
                if not data:
                    break
                result = self.upload_part(part_number, data, key)  # must raise on error
                self.parts.append(result)
                progress_callback(len(data))

            self.complete_upload(key)

        except Exception as e:
            self.log.error(f"Failed to upload large file, {e}.")
            self.abort_upload(key)
            raise

    def initiate_multipart_upload(self, key):
        response = self.client.create_multipart_upload(Bucket=self.bucket_name, Key=key)
        if 'UploadId' not in response:
            raise Exception("Failed to initiate multipart upload.")
        
        self.upload_id = response['UploadId']

    def upload_part(self, part_number, data, key, failed_count = 0):
        if self.is_token_expired():
            self.refreshToken()
        try:
            response = self.client.upload_part(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=self.upload_id,
                PartNumber=part_number,
                Body=data
            )
            return {
                'PartNumber': part_number,
                'ETag': response['ETag']
            }
        except Exception as e:
            failed_count += 1
            if failed_count > 2:
                self.log.error(f"Failed to upload part {part_number}, {e}.")
                self.abort_upload(key)
                raise
            else:
                # wait 5minutes before retrying
                time.sleep(300)  # wait for 5 minutes
                return self.upload_part(part_number, data, key, failed_count)

    def complete_upload(self, key):
        self.parts.sort(key=lambda x: x['PartNumber'])
        self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts}
        )

    def abort_upload(self, key):
        if self.upload_id:
            self.client.abort_multipart_upload(Bucket=self.bucket_name, Key=key, UploadId=self.upload_id)

    def calculate_part_size(self, file_size):
        """
        Calculate the part size based on the file size.
        The part size should be calculated for files larger than 5GB.
        """
        min_part_size = 1024 * 1024 * 10 # 10MB 
        return max(min_part_size, math.ceil(file_size / MAX_PART_NUMBER)) if file_size > SINGLE_PUT_LIMIT else file_size  
    # end manual multipart upload section

    def close(self):
        self.client.close()
        self.client = None
        self.bucket = None
        self.s3 = None

        

