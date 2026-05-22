#!/usr/bin/env python3
import os
import time
from collections import deque
from datetime import datetime
from bento.common.utils import get_logger
from common.constants import FILE_NAME_DEFAULT, SUCCEEDED, ERRORS,  OVERWRITE, DRY_RUN,\
    S3_BUCKET, TEMP_CREDENTIAL, FILE_PREFIX, RETRIES, FILE_DIR, FROM_S3, FILE_PATH,FILE_SIZE_DEFAULT, MD5_DEFAULT,\
    SUBFOLDER_FILE_NAME, TEMP_DOWNLOAD_DIR, BYPASS_ARCHIVE_VALIDATION, MAX_DELETE_RETRY
from common.utils import extract_s3_info_from_url, format_size, format_time
from common.s3util import S3Bucket
from copier import Copier
from file_validator import validate_data_file
# Line removed as ClientError is not used in the provided code snippet.

# This script upload files and matadata files from local to specified S3 bucket
# input: file info list
class FileUploader:

    # keys in job dict
    TTL = 'ttl'
    INFO = 'file_info'

    def __init__(self, configs, file_list, md5_cache, md5_cache_file, archived_files_info):
        """"
        :param configs: all configurations for file uploading
        :param file_list: list of file path, size
        """
        self.configs = configs
        self.retry= configs.get(RETRIES)
        self.prefix = configs[FILE_PREFIX]
        self.bucket_name = configs.get(S3_BUCKET)
        self.credential = configs.get(TEMP_CREDENTIAL)
        # self.pre_manifest = configs.get(PRE_MANIFEST)
        self.file_info_list = file_list
        self.archived_files_info = archived_files_info
        self.copier = None
        self.count = len(file_list)
        self.overwrite = configs.get(OVERWRITE)
        self.dryrun = configs.get(DRY_RUN)
        self.upload_log_dir = None
        self.log = get_logger('File_Uploader')
        self.from_s3 = configs.get(FROM_S3)
        self.invalid_count = 0
        # Statistics
        self.files_processed = 0
        self.files_failed = 0
        self.total_file_volume = 0
        self.md5_cache = md5_cache
        self.md5_cache_file = md5_cache_file

    """
    Set s3 bucket, prefix and file dir for downloading if source file dir is s3 url.
    """
    def _set_from_s3(self):
        if not self.from_s3: 
            self.from_bucket_name = None
            self.from_prefix = None
            self.s3_bucket = None
            self.file_dir = None
        else:
            self.file_dir = self.configs.get(FILE_DIR)
            self.from_bucket_name, self.from_prefix = extract_s3_info_from_url(self.file_dir)
            self.s3_bucket = S3Bucket()
            self.s3_bucket.set_s3_client(self.from_bucket_name, None)
    """
    Prepare file information for uploading
    :return: list of file information
    """
    def _prepare_files(self):
        files = []
        self.total_file_count = len(self.file_info_list)
        for info in self.file_info_list:
            self.total_file_volume += int(info[FILE_SIZE_DEFAULT])
            files.append({
                self.TTL: self.retry,
                self.INFO: info,
            })
            if self.files_processed >= self.count:
                break

        return files

    # Use this method in solo mode
    def upload(self):
        """
          Read file information from pre-manifest and copy them one by one to destination bucket
          :return: bool
        """
        self._set_from_s3() #reset from s3 bucket, prefix
        upload_file_list = self._prepare_files()
        if self.invalid_count > 0:
            self.log.info(f"{self.invalid_count} files are invalid and uploading skipped!")
            return False
        self.copier = Copier(self.bucket_name, self.prefix, self.configs)
        file_queue = deque(upload_file_list)
        uploaded_file_volume = 0
        self.print_start_upload_message(self.count, self.total_file_volume)
        start_uploading_at = datetime.now()
        file_count = 0
        try:
            while file_queue:
                job = file_queue.popleft()
                file_info = job[self.INFO]
                file_path = file_info[FILE_PATH]
                file_count += 1 
                job[self.TTL] -= 1
                if self.from_s3 == True: #download file from s3
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    result = self.prepare_s3_download_file(file_info, file_count, self.total_file_count)
                    if not result:
                        continue
                self.files_processed += 1
                result = self.copier.copy_file(file_info, self.overwrite, self.dryrun)
                if result.get(Copier.STATUS):
                    file_info[SUCCEEDED] = True
                    file_info[ERRORS] = None
                    if self.from_s3 == True:
                        for retry_count in range(1, MAX_DELETE_RETRY+1):
                            try:
                                os.remove(file_info[FILE_PATH])
                                break
                            except Exception as e:
                                if retry_count == MAX_DELETE_RETRY:
                                    self.log.error(f"Failed to delete temp file: {file_info[FILE_PATH]} due to {str(e)} after {MAX_DELETE_RETRY} retries.")
                                    raise e
                                self.log.error(f"Failed to delete temp file: {file_info[FILE_PATH]} due to {str(e)}")
                                #wait 30 seconds to delete temp file
                                self.log.info(f"Waiting 30 seconds to retry delete temp file: {file_info[FILE_PATH]}")
                                time.sleep(30)

                else:
                    self._deal_with_failed_file(job, file_queue)
                    if job[self.TTL]  > 0:
                        file_count -= 1

                uploaded_file_volume += file_info[FILE_SIZE_DEFAULT]
                
                # self.log.info(f'{self.copier.files_copied} out of {len(self.file_info_list)} files have been uploaded to destination.')
                self.print_progress_message(self.count, self.copier.files_copied, self.total_file_volume, uploaded_file_volume, start_uploading_at)
                   
            self.log.info(f'Files processed: {self.files_processed}')
            self.log.info(f'Files not found: {len(self.copier.files_not_found)}')
            self.log.info(f'Files copied: {self.copier.files_copied}')
            self.log.info(f'Files exist at destination: {self.copier.files_exist_at_dest}')
            self.log.info(f'Files failed: {self.files_failed}')

            if self.copier.files_exist_at_dest == self.files_processed:
                self.log.info(f"All files already exist in the cloud storage")

            return self.copier.files_copied > 0 or self.copier.files_exist_at_dest == self.files_processed
        finally:
            self.s3_bucket = None
            self.copier = None

    """
    Handle failed file uploading
    :param job: current job
    :param queue: job queue
    :return: None
    """
    def _deal_with_failed_file(self, job, queue):
        if job[self.TTL]  > 0:
            self.log.error(f'File: {job[self.INFO].get(FILE_NAME_DEFAULT) } - Uploading file FAILED! Retry left: {job[self.TTL]}')
            queue.append(job)
        else:
            self.log.critical(f'Uploading file failure exceeded maximum retry times, abort!')
            self.files_failed += 1
            file_info = job[self.INFO]
            file_info[SUCCEEDED] = False
            if self.from_s3 == True:
                os.remove(file_info[FILE_PATH])
  
    def prepare_s3_download_file(self, file_info, file_count, total_file_count):
        """
        Prepare file information for downloading from S3.

        :param file_info: Dictionary containing file information.
        :return: None
        """
        file_path = os.path.join(TEMP_DOWNLOAD_DIR, file_info[SUBFOLDER_FILE_NAME])
        file_info[FILE_PATH] = file_path
        file_key = os.path.join(self.from_prefix, file_info[FILE_NAME_DEFAULT])
        self.log.info(f"Downloading {file_info[FILE_NAME_DEFAULT]} from {self.file_dir} ...")
        try:
            result, msg = self.s3_bucket.download_object(file_key, file_path)
            if not result:
                invalid_reason = msg
                file_info[SUCCEEDED] = False
                file_info[ERRORS] = [invalid_reason]
                self.invalid_count += 1
                return False
        except Exception:      
            msg = f"Failed to download file from S3: {file_key}."
            self.log.error(msg)
            invalid_reason = msg
            file_info[SUCCEEDED] = False
            file_info[ERRORS] = [invalid_reason]
            self.invalid_count += 1
            return False
        
        self.log.info(f"{file_info[FILE_NAME_DEFAULT]} has been downloaded from {self.file_dir} successfully!")
        # validate size and md5 of downloaded data file
        result = validate_data_file(file_info, file_info.get(FILE_SIZE_DEFAULT), file_path, self.md5_cache, self.log, self.archived_files_info, self.configs.get(BYPASS_ARCHIVE_VALIDATION, False))
        if result:
            self.log.info(f'Validating file integrity succeeded on "{file_info[FILE_NAME_DEFAULT]}"')
        self.log.info(f'{file_count} out of {total_file_count} file(s) have been validated.')
        if not result:
            file_info[SUCCEEDED] = False
            os.remove(file_path)
            self.invalid_count += 1
            return False
        return True
        
    def print_start_upload_message(self, total_file_cnt, total_file_volume):
        """
        Print start message for file uploading.

        :param total_file_cnt: Total number of files to be uploaded.
        :param total_file_volume: Total volume of files in bytes.
        :return: None
        """
        self.log.info(f'Total {total_file_cnt} files ({format_size(total_file_volume)}) will be uploaded to destination.') 

    def print_progress_message(self, total_file_cnt, uploaded_file_cnt, total_file_volume, uploaded_file_volume, start_at):
        """
        Print progress message for file uploading.

        :param total_file_cnt: Total number of files to be uploaded.
        :param uploaded_file_cnt: Number of files uploaded so far.
        :param total_file_volume: Total volume of files in bytes.
        :param uploaded_file_volume: Volume of uploaded files in bytes.
        :param start_at: A datetime object representing the start time of the eclipse.
        :return: None
        """
        remaining_time = "00:00:00" if uploaded_file_cnt == total_file_cnt else calculate_remain(total_file_volume, uploaded_file_volume, start_at)
        average_speed = format_size(uploaded_file_volume/ (datetime.now() - start_at).total_seconds()) if uploaded_file_cnt > 0 else 0
        self.log.info(f'{uploaded_file_cnt} ({format_size(uploaded_file_volume)}) out of {total_file_cnt} files ({format_size(total_file_volume)}) have been uploaded to destination in {calculate_eclipse(start_at)} with average speed at {average_speed}/sec, remaining uploading time: {remaining_time}.')

"""
utile functions
"""
def calculate_eclipse(start_time):
    """
    Calculate the duration of an eclipse in hh:mm:ss format given the start time.
    
    :param start_time: A datetime object representing the start time of the eclipse.
    :return: Duration of the eclipse in hh:mm:ss format.
    """
    # Get the current time as the end time (or replace with a specific end time)
    end_time = datetime.now()
    
    # Calculate the time difference
    time_difference = end_time - start_time
    
    # Extract total seconds from the time difference
    total_seconds = int(time_difference.total_seconds())
    
    # Format the time as hh:mm:ss
    return format_time(total_seconds)

def calculate_remain(total_file_volume, uploaded_file_volume, start_at):
    """
    Calculate the remaining time for the eclipse in hh:mm:ss format given the total file volume, uploaded file volume, and start time.

    :param total_file_volume: Total volume of files in bytes.
    :param uploaded_file_volume: Volume of uploaded files in bytes.
    :param start_at: A datetime object representing the start time of the eclipse.
    :return: Remaining time for the eclipse in hh:mm:ss format.
    """
    # Calculate the remaining file volume
    remaining_file_volume = total_file_volume - uploaded_file_volume

    # Calculate the remaining time in seconds
    remaining_time_seconds = remaining_file_volume / (uploaded_file_volume / (datetime.now() - start_at).total_seconds())

    # Format the remaining time as hh:mm:ss
    return format_time(remaining_time_seconds)

       
