#!/usr/bin/env python3

import requests
import json
from bento.common.utils import get_logger

class APIInvoker:
    def __init__(self, api_token, api_url, submissionId, intention):
        self.token = api_token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.url = api_url
        self.submissionId = submissionId
        self.intention = intention
        self.log = get_logger('FileLoader')

    #to do 
    #1) get sts temp creadential for file/metadata uploading to S3 bucket
    def get_temp_credential(self):
        self.cred = {}
        body = """
        mutation {
            createTempCredentials (submissionID: self.submissionId) {
                accessKeyId,
                secretAccessKey,
                sessionToken
            }
        }
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("get_temp_credential response status code: ", response.status_code)
            if status == 200: 
                self.cred = json.load(response.content)["data"]["createTempCredentials"]
                return True
            else:
                self.log.error('Get temp creadential failed!')
                return False

        except Exception as e:
            self.log.debug(e)
            self.log.error('Get temp creadential failed! Check debug log for detailed information')
            return False


    #2) create upload batch
    def create_bitch(self, file_list):
        self.new_batch = {}
        body = """
        mutation {
            createBatch (
                submissionID: self.submissionId, 
                metadataIntention: self.intention, 
                files: file_list
            ){
                _id,
                displayID,
                submissionID,
                bucketName, # S3 bucket of the submission, for file batch / CLI use
                filePrefix, # prefix/path within S3 bucket, for file batch / CLI use
                type, # [metadata, file]
                metadataIntention,# [New, Update, Delete], Update is meant for "Update or insert", metadata only! file batches are always treated as Update
                fileCount,
                files, # array of string, only available for metadata batch
                status, # [New, Uploaded, Upload Failed, Loaded, Rejected]
                createdAt,# ISO  8601 date time format with UTC or offset e.g., 2023-05-01T09:23:30Z
                updatedAt # ISO 8601 date time format with UTC or offset e.g., 2023-05-01T09:23:30Z
            }
        }
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("create_bitch response status code: ", response.status_code)
            if status == 200: 
                self.new_batch = json.load(response.content)["data"]["createBatch"]
                return True
            else:
                self.log.error('Create batch failed!')
                return False
        except Exception as e:
            self.log.debug(e)
            self.log.error('Create batch failed! Check debug log for detailed information')
            return False

    #3) update upload batch
    def update_bitch(self, batchID, uploaded_files):
        self.batch = {}
        body = """
        mutation {
            updateBatch (batchID: batchID, files: UploadResult])
            {
                _id,
                displayID,
                submissionID,
                type, # [metadata, file]
                metadataIntention,# [New, Update, Delete], Update is meant for "Update or insert", metadata only! file batches are always treated as Update
                fileCount,
                files, # {FileInfo] array of string, only available for metadata batch
                status, # [New, Uploaded, Upload Failed, Loaded, Rejected]
                createdAt,# ISO  8601 date time format with UTC or offset e.g., 2023-05-01T09:23:30Z
                updatedAt # ISO 8601 date time format with UTC or offset e.g., 2023-05-01T09:23:30Z
            }
        }
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("create_bitch response status code: ", response.status_code)
            if status == 200: 
                self.batch = json.load(response.content)["data"]["updateBatch"]
                return True
            else:
                self.log.error('Create batch failed!')
                return False
        except Exception as e:
            self.log.debug(e)
            self.log.error('Create batch failed! Check debug log for detailed information')
            return False


    

