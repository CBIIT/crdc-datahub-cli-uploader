#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows based on the upload type, file or metadata.
#############################
import os
import sys

from bento.common.utils import get_logger, LOG_PREFIX, get_time_stamp
from common.constants import UPLOAD_TYPE, S3_BUCKET, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, BATCH_STATUS, \
    BATCH_BUCKET, BATCH, BATCH_ID, FILE_PREFIX, TEMP_CREDENTIAL, SUCCEEDED, ERRORS, BATCH_CREATED, BATCH_UPDATED, FILE_PATH
from common.graphql_client import APIInvoker
from common.utils import dump_dict_to_tsv, get_exception_msg
from upload_config import Config
from file_validator import FileValidator
from file_uploader import FileUploader

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Uploader Main'

log = get_logger('FileLoader')
# public function to received args and dispatch to different modules for different uploading types, file or metadata
def controller():  
    #step 1: process args, configuration file
    config = Config()
    if not config.validate():
        log.error("Failed to upload files: missing required valid parameter(s)!")
        print("Failed to upload files: invalid parameter(s)!  Please check log file in tmp folder for details.")
        return 1

    #step 2: validate file or metadata
    configs = config.data
    validator = FileValidator(configs)
    if not validator.validate():
        log.error("Failed to upload files: found invalid file(s)!")
        print("Failed to upload files: found invalid file(s)!  Please check log file in tmp folder for details.")
        return 1
    file_list = validator.fileList

    if validator.invalid_count == 0:
        #step 3: create a batch
        apiInvoker = APIInvoker(configs)
        file_array = [{"fileName": item[FILE_NAME_DEFAULT], "size": item[FILE_SIZE_DEFAULT]} for item in file_list]
        newBatch = None
        if apiInvoker.create_batch(file_array):
            newBatch = apiInvoker.new_batch
            if not newBatch.get(BATCH_BUCKET) or not newBatch[FILE_PREFIX] or not newBatch.get(BATCH_ID):
                log.error("Failed to upload files: can't create new batch!")
                print("Failed to upload files: can't create new batch! Please check log file in tmp folder for details.")
                return 1
            configs[S3_BUCKET] = newBatch.get(BATCH_BUCKET)
            configs[FILE_PREFIX] = newBatch[FILE_PREFIX]
            configs[BATCH_ID] = newBatch.get(BATCH_ID)
            configs[BATCH] = newBatch
            log.info(f"New batch is created: {configs[BATCH_ID]} at {newBatch[BATCH_CREATED]}")
        else:
            log.error("Failed to upload files: can't create new batch!")
            print("Failed to upload files: can't create new batch! Please check log file in tmp folder for details.")
            return 1

        #step 4: get aws sts temp credential for uploading files to s3 bucket.
        if not apiInvoker.get_temp_credential():
            log.error("Failed to upload files: can't get temp credential!")
            print("Failed to upload files: can't get temp credential! Please check log file in tmp folder for details.")
            #set fileList for update batch
            file_array = [{"fileName": item[FILE_NAME_DEFAULT], "succeeded": False, "errors": ["Failed to upload files: can't get temp credential!"]} for item in file_list]
        else:
            temp_credential = apiInvoker.cred
            configs[TEMP_CREDENTIAL] = temp_credential

            #step 5: upload all files to designated s3 bucket
            loader = FileUploader(configs, file_list)
            result = loader.upload()
            if not result:
                log.error("Failed to upload files: can't upload files to bucket!")
                print("Failed to upload files: can't upload files to bucket! Please check log file in tmp folder for details.")
            else:
                #write filelist to tsv file and save to result dir
                print("File uploading completed!")
            #set fileList for update batch
            file_array = [{"fileName": item[FILE_NAME_DEFAULT], "succeeded": item[SUCCEEDED], "errors": item[ERRORS]} for item in file_list]
        
        #step 6: update the batch
        #uploaded_files: 
        # (fileName: String
        # succeeded: Boolean
        # errors: [String])
        if apiInvoker.update_batch(newBatch[BATCH_ID], file_array):
            batch = apiInvoker.batch
            log.info(f"The batch is updated: {newBatch[BATCH_ID]} with new status: {batch[BATCH_STATUS]} at {batch[BATCH_UPDATED]} ")
        else:
            log.error(f"Failed to update batch, {newBatch[BATCH_ID]}!")
            print(f"Failed to update batch, {newBatch[BATCH_ID]}! Please check log file in tmp folder for details.")
    
    else:
        log.error(f"Found total {validator.invalid_count} file(s) are invalid!")
    
    #step 6: #dump file_list with uploading status and errors to tmp/reports dir
    try:
        file_path = f"./tmp/upload-report-{get_time_stamp()}.tsv"
        #filter out file path in the file list
        file_list = [ {i:a[i] for i in a if i!=FILE_PATH} for a in file_list]
        dump_dict_to_tsv(file_list, file_path)
        print(f"Uploading report is created at {file_path}!")
    except Exception as e:
        log.exception(f"Failed to dump uploading report files: {get_exception_msg()}.")
        print(f"Failed to dump uploading report files: {get_exception_msg()}.")
if __name__ == '__main__':
    controller()