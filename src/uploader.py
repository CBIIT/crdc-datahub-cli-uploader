#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows based on the upload type, file or metadata.
#############################
import os
import sys
from bento.common.utils import get_logger, get_log_file, get_uuid, LOG_PREFIX, UUID, get_time_stamp, removeTrailingSlash, load_plugin
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES
from upload_config import Config, UPLOAD_HELP
from file_uploader import FileLoader
from data_loader import DataLoader

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Controller'

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
        return

    #step 2: check upload type and validate file or metadata
    configs = config.data
    if configs.get(UPLOAD_TYPE) == UPLOAD_TYPES[0]: #file
        #validate files in the given local folder.
        result = False;
    elif config.data.get(UPLOAD_TYPE) == UPLOAD_TYPES[1]: #metadata
        #validate matedata.
        result = False;

    #step 3: get aws sts temp credential for uploading files to s3 bucket.

    #step 4: create a batch

    #step 5: upload all files to designated s3 bukect or load all metadata into DB
    if configs.get(UPLOAD_TYPE) == UPLOAD_TYPES[0]: #file
        loader = FileLoader(**configs)
        result = loader.upload()
    elif config.data.get(UPLOAD_TYPE) == UPLOAD_TYPES[1]: #metadata
        loader = DataLoader()
        result = loader.load()

    #step 5: update the batch

if __name__ == '__main__':
    controller()