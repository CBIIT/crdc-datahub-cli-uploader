#!/bin/env python3
import os
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TransferSpeedColumn
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from bento.common.utils import get_logger, format_bytes, removeTrailingSlash, get_md5_hex_n_base64
from common.progress_bar import create_progress_bar, ProgressCallback
from common.graphql_client import APIInvoker
from common.s3util import S3Bucket
from common.constants import UPLOAD_TYPE, TYPE_FILE, TYPE_MATE_DATA, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, TEMP_CREDENTIAL, FILE_PATH, \
    ERRORS, SKIPPED
from common.utils import get_exception_msg

class Copier:

    TRANSFER_UNIT_MB = 1024 * 1024
    MULTI_PART_THRESHOLD = 100 * TRANSFER_UNIT_MB
    MULTI_PART_CHUNK_SIZE = MULTI_PART_THRESHOLD
    PARTS_LIMIT = 900
    SINGLE_PUT_LIMIT = 4_500_000_000

    # keys for copy result dict
    STATUS = 'status'
    SIZE = 'size'
    KEY = 'key'
    NAME = 'name'
    FIELDS = 'fields'
    ACL = 'acl'

    def __init__(self, bucket_name, prefix, configs):

        """"
        Copy file from URL or local file to S3 bucket
        :param bucket_name: string type
        """
        self.configs = configs
        if not bucket_name:
            raise ValueError('Empty destination bucket name')
        self.bucket_name = bucket_name
        self.bucket = S3Bucket()
        #set s3 client based on credential and bucket.
        self.bucket.set_s3_client(self.bucket_name, configs[TEMP_CREDENTIAL])
         
        if prefix and isinstance(prefix, str):
            self.prefix = removeTrailingSlash(prefix)
        else:
            raise ValueError(f'Invalid prefix: "{prefix}"')
       
        self.log = get_logger('Copier')
        self.files_exist_at_dest = 0
        self.files_copied = 0
        self.files_not_found = set()
        self.type = configs.get(UPLOAD_TYPE)

    def refreshToken(self):
        apiInvoker = APIInvoker(self.configs)
        if apiInvoker.get_temp_credential():
            temp_credential = apiInvoker.cred
            self.configs[TEMP_CREDENTIAL] = temp_credential
            return True
        else:
            self.log.error("Failed to upload files: can't refresh temp credential!")
            return False
    

    def set_prefix(self, raw_prefix):
        prefix = removeTrailingSlash(raw_prefix)
        if prefix != self.prefix:
            self.prefix = prefix

    def copy_file(self, file_info, overwrite, dryrun):
        """
        Copy a file to S3 bucket
        :param file_info: dict that has file information
        :param overwrite: overwrite file in S3 bucket even existing file has same size
        :param dryrun: only do preliminary check, don't copy file
        :return: dict
        """
        try:
            org_url = file_info[FILE_PATH]
            file_name = file_info[FILE_NAME_DEFAULT]
            self.log.info(f'Processing {org_url}')
            key = f'{self.prefix}/{file_name}'
            org_size = file_info[FILE_SIZE_DEFAULT]
            self.log.info(f'Original file size: {format_bytes(org_size)}.')

            succeed = {self.STATUS: True,
                       self.NAME: file_name,
                       self.KEY: key,
                       self.ACL: None,
                       self.SIZE: org_size
                       }

            if dryrun:           
                self.log.info(f'Uploading “{file_name}” skipped (dry run)')
                return succeed
            
            if not overwrite and self.bucket.same_size_file_exists(key, org_size):
                self.log.info(f'Uploading “{file_name}” skipped - file with same name and size already exists in the cloud storage')
                self.files_exist_at_dest += 1
                file_info[SKIPPED] = True
                return succeed
            else: 
                file_info[SKIPPED] = False
            #self.log.info(f'Copying from {org_url} to s3://{self.bucket_name}/{key.strip("/")} ...')
            self.log.info(f'Uploading file, "{org_url}" to destination...')
            dest_size = self._upload_obj(org_url, key, org_size)
            if dest_size != org_size:
                self.log.error(f'Uploading “{file_name}” failed - uploading was not complete. Please try again and contact the helpdesk if this error persists.')
                return {self.STATUS: False}
            
            return succeed
        except ClientError as ce:
            self.log.debug(ce)

            #handle temp credential expired error to refresh token for next file.
            if ce.response[u'Error'][u'Code'] == 'ExpiredToken':
                self.log.exception(f'Uploading “{file_name}” failed - internal error: temporary credential expired. Please try again and contact the helpdesk if this error persists.')
                file_info[ERRORS] = [f'Uploading “{file_name}” failed - internal error: temporary credential expired.']
                if self.refreshToken(): 
                    self.bucket.set_s3_client(self.bucket_name, self.configs[TEMP_CREDENTIAL])
            else:
                self.log.exception(f"Uploading “{file_name}” failed - internal error. Please try again and contact the helpdesk if this error persists..")
                file_info[ERRORS] = [f'Uploading “{file_name}” failed - network error.']

            return {self.STATUS: False}
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Uploading “{file_name}” failed - network error. Please try again and contact the helpdesk if this error persists.')
            file_info[ERRORS] = [f"Uploading “{file_name}” failed - internal error."]
            return {self.STATUS: False}

    def _upload_obj(self, org_url, key, org_size):
        if self.type == TYPE_FILE or org_size > self.SINGLE_PUT_LIMIT: #study files upload (big files)

            parts = int(org_size) // self.MULTI_PART_CHUNK_SIZE
            chunk_size = self.MULTI_PART_CHUNK_SIZE if parts < self.PARTS_LIMIT else int(org_size) // self.PARTS_LIMIT
            t_config = TransferConfig(multipart_threshold=self.MULTI_PART_THRESHOLD,
                                        multipart_chunksize=chunk_size)
            with open(org_url, 'rb') as stream, create_progress_bar() as progress:
                task_id = progress.add_task("Uploading...", total=org_size)
                progress_callback = ProgressCallback(org_size, progress, task_id)
                self.bucket.upload_file_obj(stream, key, progress_callback, t_config)
        else: #small file
            md5_obj = get_md5_hex_n_base64(org_url)
            md5_base64 = md5_obj['base64']
            with open(org_url, 'rb') as data:
                self.bucket.put_file_obj(org_size, key, data, md5_base64 )
            
        self.files_copied += 1
        size, msg =  self.bucket.get_object_size(key)
        return size
