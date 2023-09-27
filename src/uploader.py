#!/usr/bin/env python3
#########Uploader.py#########
#The entry point of the cli, it control the workflows based on the upload type, file or metadata.
#############################
import os
import sys
from bento.common.utils import get_logger, LOG_PREFIX
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES, S3_BUCKET, API_URL, SUBMISSION_ID, INTENTION, BUCKET, FILE_PREFIX, TEMP_CREDENTIAL
from common.s3util import get_temp_creadential
from common.graphql_client import APIInvoker
from upload_config import Config, UPLOAD_HELP
from file_validator import FileValidator
from file_uploader import FileLoader

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Uploader Main'

log = get_logger('FileLoader')
# public function to received args and dispitch to different modules for different uploading types, file or metadata
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
        print("Failed to upload files: invalid parameter(s)!  Please check log file in tmp folder for details.")
        return

    #step 2: validate file or metadata
    configs = config.data
    upload_type = configs.get(UPLOAD_TYPE)
    validator = FileValidator(configs)
    if not validator.validate():
        log.error("Failed to upload files: found invalid file(s)!")
        print("Failed to upload files: found invalid file(s)!  Please check log file in tmp folder for details.")
        return
    file_list = validator.fileList
    
    #step 3: create a batch
    apiInvoker = APIInvoker(configs)
    # newBatch = {}  #API is not ready for integration
    # file-array = []
    # if apiInvoker.create_bitch(file-array):
    #     newBatch = apiInvoker.new_batch
    #     configs[S3_BUCKET] = newBatchBUCKET]
    #     configs[FILE_PREFIX] = newBatch[FILE_PREFIX]
    #     if upload_type ==  UPLOAD_TYPES[1]:
    #        configs["presignedUrls"] = newBatch["files"]
    # else:
    #     log.error("Failed to upload files: can't create new batch!")
    #     print("Failed to upload files: can't create new batch! Please check log file in tmp folder for details.")
    #     return
    configs[S3_BUCKET] = "crdcdh-test-submission" #test code 
    configs[FILE_PREFIX] = "123456/file" if upload_type == UPLOAD_TYPES[0] else "123456/metadata" #test code 

    #step 4: get aws sts temp credential for uploading files to s3 bucket.
    if apiInvoker.get_temp_credential():
        temp_credential = apiInvoker.cred
        configs[TEMP_CREDENTIAL] = temp_credential

    else:
        log.error("Failed to upload files: can't get temp credential!")
        print("Failed to upload files: can't get temp credential! Please check log file in tmp folder for details.")
        return

    #step 5: upload all files to designated s3 bukect or load all metadata into DB
    loader = FileLoader(configs, file_list)
    result = loader.upload()
    #print(result)
    if not result:
        log.error("Failed to upload files: can't upload files to bukect!")
        print("Failed to upload files: can't upload files to bukect! Please check log file in tmp folder for details.")
        return
    else:
        #write filelist to tsv file and save to result dir
        print("File uploading completed!")
    
    #step 5: update the batch
    # if apiInvoker.update_bitch(validator.fileList):
    #     batch = apiInvoker.batch
    # else:
    #     log.error(f"Failed to update batch, {newBatch['_id']}")
    #     print(f"Failed to update batch, {newBatch['_id']} Please check log file in tmp folder for details.")
    #     return

if __name__ == '__main__':
    controller()