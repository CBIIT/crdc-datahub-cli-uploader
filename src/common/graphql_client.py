#!/usr/bin/env python3

import requests
import json
from bento.common.utils import get_logger
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES, API_URL, SUBMISSION_ID, INTENTION, TOKEN

class APIInvoker:
    def __init__(self, configs):
        self.token = configs.get(TOKEN)
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.url = configs.get(API_URL)
        self.submissionId = configs.get(SUBMISSION_ID)
        self.intention = configs.get(INTENTION)
        self.log = get_logger('GraphQL API')

    #to do 
    #1) get sts temp creadential for file/metadata uploading to S3 bucket
    def get_temp_credential(self):
        self.cred = {}
        body = f"""
        mutation {{
            createTempCredentials (submissionID: \"{self.submissionId}\") {{
                accessKeyId,
                secretAccessKey,
                sessionToken
            }}
        }}
        """
        #print(body)
        # print(self.headers)
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("get_temp_credential response status code: ", response.status_code)
            if status == 200: 
                results = json.loads(response.content)
                if results.get("data"):
                    self.cred = results.get("data").get("createTempCredentials")
                    return True
                else:
                    if results.get("errors"):
                        self.log.error(f'Get temp creadential failed: {results.get("errors").get("message")}!')  
                    else:
                        self.log.error('Get temp creadential failed!')
                    return False
               
            else:
                self.log.error(f'Get temp creadential failed with status code: {status}')
                return False

        except Exception as e:
            self.log.debug(e)
            self.log.exception('Get temp creadential failed! Check debug log for detailed information')
            return False


    #2) create upload batch
    def create_bitch(self, file_list):
        self.new_batch = {}
        body = f"""
        mutation {{
            createBatch (
                submissionID: \"{self.submissionId}\", 
                metadataIntention: \"{self.intention}\", 
                files: {file_list}
            ){{
                _id,
                displayID,
                submissionID,
                bucketName,
                filePrefix,
                type,
                metadataIntention,
                fileCount,
                files,
                status,
                createdAt,
                updatedAt
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("create_bitch response status code: ", response.status_code)
            if status == 200: 
                results = json.loads(response.content)
                if results.get("data"):
                    self.new_batch = results.get("data").get("createBatch")
                    return True
                else:
                    if results.get("errors"):
                        self.log.error(f'Create batch failed: {results.get("errors").get("message")}!')  
                    else:
                        self.log.error('Create batch failed!')
                    return False
               
            else:
                self.log.error(f'Create batch failed with status code: {status}')
                return False
            
        except Exception as e:
            self.log.debug(e)
            self.log.exception('Create batch failed! Check debug log for detailed information')
            return False

    #3) update upload batch
    def update_bitch(self, batchID, uploaded_files):
        self.batch = {}
        body = f"""
        mutation {{
            updateBatch (
                batchID: \"{batchID}\", 
                files: {uploaded_files}
                )
            {{
                _id,
                displayID,
                submissionID,
                type, # [metadata, file]
                metadataIntention,# [New, Update, Delete], Update is meant for "Update or insert", metadata only! file batches are always treated as Update
                fileCount,
                files,
                status,
                createdAt,
                updatedAt
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info("create_bitch response status code: ", response.status_code)
            if status == 200: 
                results = json.loads(response.content)
                if results.get("data"):
                    self.new_batch = results.get("data").get("updateBatch")
                    return True
                else:
                    if results.get("errors"):
                        self.log.error(f'Update batch failed: {results.get("errors").get("message")}!')  
                    else:
                        self.log.error('Update batch failed!')
                    return False
                
            else:
                self.log.error(f'Update batch failed with status code: {status}')
                return False
        except Exception as e:
            self.log.debug(e)
            self.log.error('Update batch failed! Check debug log for detailed information')
            return False


    

