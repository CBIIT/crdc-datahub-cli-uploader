#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows based on the upload type, file or metadata.
#############################
import os
import sys
from bento.common.utils import get_logger, get_log_file, get_uuid, LOG_PREFIX, UUID, get_time_stamp, removeTrailingSlash, load_plugin
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES, S3_BUCKET, FILE_INVALID_REASON
from common.s3util import get_temp_creadential
from upload_config import Config, UPLOAD_HELP
from file_validator import FileValidator
from file_uploader import FileLoader
from data_loader import DataLoader

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Uploader Main'

log = get_logger('FileLoader')
# public function to received the args and dispitch to different modules for different uploading types, file or metadata
def controller():
    #allow end user to get help from the cli
    args = sys.argv[1:]
    if len(args) == 1 and (args[0] == "help" or args[0] == "-help")  or args[0] == "-h":
        print(UPLOAD_HELP)
        return
    
    #step 1: process args, configuration file
    config = Config()
    if not config.validate():
        log.error("Failed to upload files: missing required valid parameter(s)!")
        print("Failed to upload files: missing required valid parameter(s)!  Please check log file in tmp folder for details.")
        return

    #step 2: validate file or metadata
    configs = config.data
    validator = FileValidator(configs)
    if not validator.validate():
        log.error("Failed to upload files: found invalid file(s)!")
        print("Failed to upload files: found invalid file(s)!  Please check log file in tmp folder for details.")
        return
    file_list = validator.fileList
    field_names = validator.field_names #name array

    #step 3: create a batch


    #step 4: get aws sts temp credential for uploading files to s3 bucket.
    temp_credential = get_temp_creadential("420434175168", "crdcdh-test-submission") # test codes

    configs[S3_BUCKET] = "dhloadertest" #debug code
    #step 5: upload all files to designated s3 bukect or load all metadata into DB
    if configs.get(UPLOAD_TYPE) == UPLOAD_TYPES[0]: #file
        valid_file_list = [file for file in file_list if not file[FILE_INVALID_REASON] ]
        invalid_file_list = [file for file in file_list if file[FILE_INVALID_REASON]]
        loader = FileLoader(configs, valid_file_list, field_names)
        result = loader.upload()
        #print(result)
    elif config.data.get(UPLOAD_TYPE) == UPLOAD_TYPES[1]: #metadata
        loader = DataLoader(validator.fileList)
        result = loader.load()

    
    #step 5: update the batch

if __name__ == '__main__':
    controller()