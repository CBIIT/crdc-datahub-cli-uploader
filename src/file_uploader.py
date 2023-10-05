#!/usr/bin/env python3
import os
from collections import deque

from bento.common.utils import get_logger, LOG_PREFIX
from copier import Copier
from bento.common.s3 import upload_log_file
from common.constants import UPLOAD_TYPE, FILE_NAME_DEFAULT, SUCCEEDED, ERRORS,  \
    SUBMISSION_ID, PRE_MANIFEST, S3_BUCKET, TEMP_CREDENTIAL, FILE_PREFIX, RETRIES
from common.utils import clean_up_strs, clean_up_key_value

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_Loader'

# This script upload files and matadatafiles from local to specified S3 bucket
# input: file info list
class FileLoader:

    # keys in job dict
    TTL = 'ttl'
    INFO = 'file_info'

    def __init__(self, configs, file_list):
        """"
        :param configs: all configurations for file uploading
        :param file_list: list of file path, size
        """
        dryrun = False
        overwrite = True

        self.configs = configs
        self.retry= configs.get(RETRIES)
        if configs.get(FILE_PREFIX):
            self.prefix = configs[FILE_PREFIX]
        else:
            self.prefix =  f'{configs[SUBMISSION_ID]}/{configs[UPLOAD_TYPE]}' # prefix is submissionId/file
        self.bucket_name = configs.get(S3_BUCKET)
        self.credential = configs.get(TEMP_CREDENTIAL)
        self.pre_manifest = configs.get(PRE_MANIFEST)
        self.file_info_list = file_list
        self.copier = None
        self.count = len(file_list)
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.upload_log_dir = None
        self.log = get_logger('FileLoader')

        # Statistics
        self.files_processed = 0
        self.files_skipped = 0
        self.files_failed = 0

    @staticmethod
    def get_s3_location(bucket, key):
        return "s3://{}/{}".format(bucket, key)

    def _prepare_files(self):
        files = []
        for info in self.file_info_list:
            files.append({
                self.TTL: self.retry,
                self.INFO: info,
            })
            if self.files_processed >= self.count > 0:
                break
        return files

    # Use this method in solo mode
    def upload(self):
        """
          Read file information from pre-manifest and copy them all to destination bucket
          :return:
        """

        self.copier = Copier(self.bucket_name, self.prefix, self.configs)

        file_queue = deque(self._prepare_files())

        while file_queue:
            job = file_queue.popleft()
            file_info = job[self.INFO]
            #skip invalid file
            file_skip = False if not file_info.get(SUCCEEDED) else True
            job[self.TTL] -= 1
            if file_skip == False:
                self.files_processed += 1
                result = self.copier.copy_file(file_info, self.overwrite, self.dryrun)
                if result.get(Copier.STATUS):
                    file_info[SUCCEEDED] = True
                    file_info[ERRORS] = None
                else:
                    self._deal_with_failed_file(job, file_queue)
            else:
                self.files_skipped += 1
                file_info[SUCCEEDED] = False

        self.log.info(f'Files skipped: {self.files_skipped}')
        self.log.info(f'Files processed: {self.files_processed}')
        self.log.info(f'Files not found: {len(self.copier.files_not_found)}')
        self.log.info(f'Files copied: {self.copier.files_copied}')
        self.log.info(f'Files exist at destination: {self.copier.files_exist_at_dest}')
        self.log.info(f'Files failed: {self.files_failed}')

        return self.copier.files_copied > 0

    def _deal_with_failed_file(self, job, queue):
        if job[self.TTL]  > 0:
            self.log.error(f'File: {job[self.INFO].get(FILE_NAME_DEFAULT) } - Uploading file FAILED! Retry left: {job[self.TTL]}')
            queue.append(job)
        else:
            self.log.critical(f'Uploading file failure exceeded maximum retry times, abort!')
            self.files_failed += 1
            file_info = job[self.INFO]
            file_info[SUCCEEDED] = False
       
