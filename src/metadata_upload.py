#!/usr/bin/env python3

import csv
import json
import os
from collections import deque

from bento.common.sqs import Queue, VisibilityExtender
from bento.common.utils import get_logger, get_log_file, get_uuid, LOG_PREFIX, UUID, get_time_stamp, removeTrailingSlash, load_plugin
from copier import Copier
from upload_config import Config
from bento.common.s3 import upload_log_file
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES,INTENTION, INTENTIONS, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    SUBMISSION_ID, FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD,S3_BUCKET, UPLOAD_STATUS
from common.utils import clean_up_strs, clean_up_key_value

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_Loader'

# This script copies (stream in memory) files from an URL to specified S3 bucket
#
# Inputs:
#   pre-manifest: TSV file that contains all information of original files
#   target bucket:


class FileLoader:
    GUID = 'GUID'
    MD5 = 'md5'
    SIZE = 'size'
    URL = 'url'
    MANIFEST_FIELDS = [GUID, MD5, SIZE, Copier.ACL, URL]

    NODE_TYPE = 'type'
    FILE_NAME = 'file_name'
    FILE_SIZE = "file_size"
    MD5_SUM = 'md5sum'
    FILE_STAT = 'file_status'
    FILE_LOC = 'file_location'
    FILE_FORMAT = 'file_format'
    DATA_FIELDS = [NODE_TYPE, FILE_NAME, UUID, FILE_SIZE, MD5_SUM, FILE_STAT, FILE_LOC, FILE_FORMAT, Copier.ACL]

    DEFAULT_NODE_TYPE = 'file'
    DEFAULT_STAT = 'uploaded'
    INDEXD_GUID_PREFIX = 'dg.4DFC/'
    INDEXD_MANIFEST_EXT = '.tsv'
    VISIBILITY_TIMEOUT = 30

    # keys in job dict
    TTL = 'ttl'
    INFO = 'file_info'
    LINE = 'line_num'
    OVERWRITE = 'overwrite'
    DRY_RUN = 'dry_run'
    BUCKET = 'bucket'
    PREFIX = 'prefix'
    VERIFY_MD5 = 'verify_md5'
    LOG_UPLOAD_DIR = 'upload_log_dir'

    def __init__(self, configs, file_list, field_names):
        """"

        :param configs: all configurations for file uploading
        :param file_list: list of file path, size

        """
        retry=3
        verify_md5=False
        dryrun=False
        overwrite=False


        self.prefix = f'{configs[SUBMISSION_ID]}/{UPLOAD_TYPES[0]}' # prefix is submissionId/file
        self.bucket_name = configs[S3_BUCKET]
        self.pre_manifest = configs[PRE_MANIFEST]
        self.file_info_list = file_list
        self.field_names = field_names
        self.copier = None
        self.count = len(file_list)
        self.domain = "caninecommons.cancer.gov"

        if not isinstance(retry, int) and retry > 0:
            raise ValueError(f'Invalid retry value: {retry}')
        self.retry = retry
        if not isinstance(overwrite, bool):
            raise TypeError(f'Invalid overwrite value: {overwrite}')
        self.overwrite = overwrite
        if not isinstance(dryrun, bool):
            raise TypeError(f'Invalid dryrun value: {dryrun}')
        self.dryrun = dryrun
        self.verify_md5 = verify_md5
        self.upload_log_dir = None

        self.log = get_logger('FileLoader')

        # Statistics
        self.files_processed = 0
        self.files_skipped = 0
        self.files_failed = 0

    def get_indexd_manifest_name(self, file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, _ = os.path.splitext(base_name)
        new_name = '{}_indexd{}'.format(name, self.INDEXD_MANIFEST_EXT)
        return os.path.join(folder, new_name)

    @staticmethod
    def get_s3_location(bucket, key):
        return "s3://{}/{}".format(bucket, key)

    @staticmethod
    def get_neo4j_manifest_name(file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, ext = os.path.splitext(base_name)
        new_name = '{}_neo4j{}'.format(name, ext)
        return os.path.join(folder, new_name)

    def populate_indexd_record(self, record, result):
        record[self.SIZE] = result[Copier.SIZE]
        record[self.MD5] = result[Copier.MD5]
        record[Copier.ACL] = result[Copier.ACL]
        record[self.URL] = self.get_s3_location(self.bucket_name, result[Copier.KEY])
        record[self.GUID] = '{}{}'.format(self.INDEXD_GUID_PREFIX, get_uuid(self.domain, "file", record[self.URL]))
        return record

    def populate_neo4j_record(self, record, result):
        if self.NODE_TYPE not in record:
            record[self.NODE_TYPE] = self.DEFAULT_NODE_TYPE
        record[self.FILE_NAME] = result[Copier.NAME]
        record[self.FILE_SIZE] = result[Copier.SIZE]
        record[self.FILE_LOC] = self.get_s3_location(self.bucket_name, result[Copier.KEY])
        file_name = result[Copier.NAME]
        record[self.MD5_SUM] = result[Copier.MD5]
        record[self.FILE_FORMAT] = (os.path.splitext(file_name)[1]).split('.')[1].lower()
        record[UUID] = get_uuid(self.domain, "file", record[self.FILE_LOC])
        record[self.FILE_STAT] = self.DEFAULT_STAT
        record[Copier.ACL] = result[Copier.ACL]
        return record
    
    def _read_pre_manifest(self):
        files = []
        line_num = self.files_skipped + 1
        for info in self.file_info_list:
            self.files_processed += 1
            line_num += 1
            files.append({
                self.LINE: line_num,
                self.TTL: self.retry,
                self.OVERWRITE: self.overwrite,
                self.DRY_RUN: self.dryrun,
                self.INFO: info,
                self.BUCKET: self.bucket_name,
                self.PREFIX: self.prefix,
                self.VERIFY_MD5: self.verify_md5
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

        self.copier = Copier(self.bucket_name, self.prefix)

        file_queue = deque(self._read_pre_manifest())
        indexd_manifest = self.get_indexd_manifest_name(self.pre_manifest)
        neo4j_manifest = self.get_neo4j_manifest_name(self.pre_manifest)

        with open(indexd_manifest, 'w', newline='\n') as indexd_f:
            indexd_writer = csv.DictWriter(indexd_f, delimiter='\t', fieldnames=self.MANIFEST_FIELDS)
            indexd_writer.writeheader()
            with open(neo4j_manifest, 'w', newline='\n') as neo4j_f:
                fieldnames = self.DATA_FIELDS
                for field in self.field_names:
                    if field not in fieldnames:
                        fieldnames.append(field)
                neo4j_writer = csv.DictWriter(neo4j_f, delimiter='\t', fieldnames=fieldnames)
                neo4j_writer.writeheader()

                while file_queue:
                    job = file_queue.popleft()
                    file_info = job[self.INFO]
                    file_skip = False
                    job[self.TTL] -= 1
                    if file_skip == False:
                        try:
                            result = self.copier.copy_file(file_info, self.overwrite, self.dryrun, self.field_names, self.verify_md5)
                            if result[Copier.STATUS]:
                                indexd_record = {}
                                self.populate_indexd_record(indexd_record, result)
                                indexd_writer.writerow(indexd_record)
                                neo4j_record = {key: None for key in result[Copier.FIELDS]}
                                self.populate_neo4j_record(neo4j_record, result)
                                neo4j_writer.writerow(neo4j_record)
                                file_info[Copier.STATUS] = "Completed"
                            else:
                                self._deal_with_failed_file(job, file_queue)
                        except Exception as e:
                            self.log.debug(e)
                            self._deal_with_failed_file(job, file_queue)
                    else:
                        self.files_skipped += 1
                if self.files_skipped > 0:
                    self.log.info(f'Files skipped: {self.files_skipped}')
                self.log.info(f'Files processed: {self.files_processed}')
                self.log.info(f'Files not found: {len(self.copier.files_not_found)}')
                self.log.info(f'Files copied: {self.copier.files_copied}')
                self.log.info(f'Files exist at destination: {self.copier.files_exist_at_dest}')
                self.log.info(f'Files failed: {self.files_failed}')

        #upload log file into configured upload_log_dir
        ori_log_file = get_log_file()
        if self.upload_log_dir:
            try:
                upload_log_file(self.upload_log_dir, ori_log_file)
                self.log.error(f'Uploading log file {ori_log_file} succeeded!')
            except Exception as e:
                self.log.debug(e)
                self.log.error(f'Uploading log file {ori_log_file} failed!')

        return self.files_failed == 0

    def _deal_with_failed_file(self, job, queue):
        if job[self.TTL]  > 0:
            self.log.error(f'Line: {job[self.LINE]} - Uploading file FAILED! Retry left: {job[self.TTL]}')
            queue.append(job)
        else:
            self.log.critical(f'Uploading file failure exceeded maximum retry times, abort!')
            self.files_failed += 1
            file_info = job[self.INFO]
            file_info[UPLOAD_STATUS] = "Failed"

    def _deal_with_failed_file_sqs(self, job):
        self.log.info(f'Upload file FAILED, {job[self.TTL] - 1} retry left!')
        job[self.TTL] -= 1
        self.job_queue.sendMsgToQueue(job, f'{job[self.LINE]}_{job[self.TTL]}')
       
