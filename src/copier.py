#!/bin/env python3
import os
import re

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import requests

from common.graphql_client import APIInvoker

from bento.common.utils import get_logger, format_bytes, removeTrailingSlash, stream_download, get_md5
from common.s3util import S3Bucket
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES,INTENTION, INTENTIONS, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    TOKEN, SUBMISSION_ID, TEMP_CREDENTIAL, S3_BUCKET


def _is_valid_url(org_url):
    return re.search(r'^[^:/]+://', org_url)


def _is_local(org_url):
    return org_url.startswith('file://')


def _get_local_path(org_url):
    if _is_local(org_url):
        return org_url.replace('file://', '')
    else:
        raise ValueError(f'{org_url} is not a local file!')


def _get_org_md5(org_url, local_file):
    """
    Get original MD5, if adapter can't get it, calculate it from original file, download if necessary
    :param org_url:
    :return:
    """
    if _is_local(org_url):
        file_path = _get_local_path(org_url)
        return get_md5(file_path)
    else:
        # Download to local and calculate MD5
        stream_download(org_url, local_file)
        if not os.path.isfile(local_file):
            raise Exception(f'Download file {org_url} to local failed!')
        return get_md5(local_file)


class Copier:
    adapter_attrs = ['load_file_info', 'clear_file_info', 'get_org_url', 'get_file_name', 'get_org_md5',
                     'filter_fields', 'get_fields', 'get_acl', 'get_org_size']

    TRANSFER_UNIT_MB = 1024 * 1024
    MULTI_PART_THRESHOLD = 100 * TRANSFER_UNIT_MB
    MULTI_PART_CHUNK_SIZE = MULTI_PART_THRESHOLD
    PARTS_LIMIT = 900

    # keys for copy result dict
    STATUS = 'status'
    SIZE = 'size'
    MD5 = 'md5'
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

    def copy_file(self, file_info, overwrite, dryrun, field_names, verify_md5=False):
        """
        Copy a file to S3 bucket
        :param file_info: dict that has file information
        :param overwrite: overwrite file in S3 bucket even existing file has same size
        :param dryrun: only do preliminary check, don't copy file
        :param verify_md5: verify file size and MD5 in file_info against original file
        :return: dict
        """
        local_file = None
        try:
            org_url = file_info[FILE_NAME_DEFAULT]
            file_name = os.path.basename(org_url)
            self.log.info(f'Processing {org_url}')
            key = f'{self.prefix}/{file_name}'
            org_size = file_info[FILE_SIZE_DEFAULT]
            self.log.info(f'Original file size: {format_bytes(org_size)}.')
            org_md5 = file_info[MD5_DEFAULT]
            self.log.info(f'Original MD5 {org_md5}')

            succeed = {self.STATUS: True,
                       self.MD5: org_md5,
                       self.NAME: file_name,
                       self.KEY: key,
                       self.FIELDS: field_names,
                       self.ACL: None,
                       self.SIZE: org_size
                       }

            if dryrun:           
                self.log.info(f'Copying file {key} skipped (dry run)')
                return succeed
            
            if not overwrite and self.bucket.same_size_file_exists(key, org_size):
                self.log.info(f'File skipped: same size file exists at: "{key}"')
                self.files_exist_at_dest += 1
                return succeed

            self.log.info(f'Copying from {org_url} to s3://{self.bucket_name}/{key} ...')
            # Original file is local
            dest_size = self._upload_obj(org_url, key, org_size)
            if dest_size != org_size:
                self.log.error(f'Copy failed: destination file size is different from original!')
                return {self.STATUS: False}

            return succeed
        except ClientError as ce:
            self.log.exception(f"Failed uploading file,{file_name} to {self.bucket_name}!", ce)
            #handle temp credential expired error to refresh token for next file.
            if ce.response[u'Error'][u'Code'] == 'ExpiredToken':
                self.log.error(u"AWS Token expired!")
                if self.refreshToken(): 
                    self.bucket.set_s3_client(self.bucket_name, self.configs[TEMP_CREDENTIAL])

            return {self.STATUS: False}
        except Exception as e:
            self.log.debug(e)
            self.log.error('Copy file failed! Check debug log for detailed information')
            return {self.STATUS: False}
        finally:
            if local_file and os.path.isfile(local_file):
                os.remove(local_file)

    def _upload_obj(self, org_url, key, org_size):
        parts = int(org_size) // self.MULTI_PART_CHUNK_SIZE
        chunk_size = self.MULTI_PART_CHUNK_SIZE if parts < self.PARTS_LIMIT else int(org_size) // self.PARTS_LIMIT
        t_config = TransferConfig(multipart_threshold=self.MULTI_PART_THRESHOLD,
                                    multipart_chunksize=chunk_size)
        with open(org_url, 'rb') as stream:
            self.bucket._upload_file_obj(key, stream, t_config)
        self.files_copied += 1
        self.log.info(f'Copying file {key} SUCCEEDED!')
        return self.bucket.get_object_size(key)

    def _file_exists(self, org_url):
        if _is_local(org_url):
            file_path = _get_local_path(org_url)
            if not os.path.isfile(file_path):
                self.log.error(f'"{file_path}" is not a file!')
                self.files_not_found.add(org_url)
                return False
            else:
                return True
        else:
            with requests.head(org_url) as r:
                if r.ok:
                    return True
                elif r.status_code == 404:
                    self.log.error(f'File not found: {org_url}!')
                    self.files_not_found.add(org_url)
                else:
                    self.log.error(f'Head file error - {r.status_code}: {org_url}')
                return False
