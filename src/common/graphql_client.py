#!/usr/bin/env python3

import requests
import json
from bento.common.utils import get_logger
from common.constants import UPLOAD_TYPE, API_URL, SUBMISSION_ID, INTENTION, TOKEN
from common.utils import get_exception_msg

class APIInvoker:
    def __init__(self, configs):
        self.token = configs.get(TOKEN)
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.url = configs.get(API_URL)
        self.submissionId = configs.get(SUBMISSION_ID)
        self.intention = configs.get(INTENTION)
        self.log = get_logger('GraphQL API')
        self.type = configs.get(UPLOAD_TYPE)

    #1) get sts temp credential for file/metadata uploading to S3 bucket
    def get_temp_credential(self):
        self.cred = None
        body = f"""
        mutation {{
            createTempCredentials (submissionID: \"{self.submissionId}\") {{
                accessKeyId,
                secretAccessKey,
                sessionToken
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info(f"get_temp_credential response status code: {status}.")
            if status == 200: 
                results = response.json()
                if results.get("errors"):
                    self.log.error(f'Get temp credential failed: {results.get("errors").get("message")}!')  
                    return False
                else:
                    self.cred = results.get("data").get("createTempCredentials")
                    return True  
            else:
                self.log.error(f'Get temp credential failed with status code: {status}.')
                return False

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Get temp credential failed! {get_exception_msg()}')
            return False


    #2) create upload batch
    def create_batch(self, file_array):
        self.new_batch = None
        #adjust file list to match the graphql param.
        file_array = json.dumps(file_array).replace("\"fileName\"", "fileName").replace("\"size\"", "size")
        intention = "null" if not self.intention else "\"" + self.intention + "\"" 
        body = f"""
        mutation {{
            createBatch (
                submissionID: \"{self.submissionId}\", 
                type: \"{self.type}\", 
                metadataIntention: {intention} 
                files: {file_array}
            ){{
                _id,
                submissionID,
                bucketName,
                filePrefix,
                type,
                metadataIntention,
                fileCount,
                status,
                createdAt
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info(f"create batch response status code: {status}.")
            if status == 200: 
                results = response.json()
                if results.get("errors"):
                        self.log.error(f'Create batch failed: {results.get("errors")[0].get("message")}!') 
                else:
                    self.new_batch = results.get("data").get("createBatch")
                    if self.new_batch:
                        return True
                    else:
                        self.log.error('Create batch failed!')
                        return False
            else:
                self.log.error(f'Create batch failed with status code: {status}.')
                return False
            
        except Exception as e:
            #self.log.debug(e)
            self.log.exception(f'Create batch failed! {get_exception_msg()}')
            return False

    #3) update upload batch
    def update_batch(self, batchID, uploaded_files):
        self.batch = None
        #adjust file list to match the graphql param.
        file_array = json.dumps(uploaded_files).replace("\"fileName\"", "fileName").replace("\"succeeded\"", "succeeded").replace("\"errors\"", "errors").replace("\"skipped\"", "skipped") \
            if uploaded_files and len(uploaded_files) > 0 else json.dumps(uploaded_files)
        body = f"""
        mutation {{
            updateBatch (
                batchID: \"{batchID}\", 
                files: {file_array}
                )
            {{
                _id,
                submissionID,
                type,
                metadataIntention,
                fileCount,
                status,
                updatedAt
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info(f"update batch response status code: {status}.")
            if status == 200: 
                results = response.json()
                if results.get("errors"):
                        self.log.error(f'Update batch failed: {results.get("errors")[0].get("message")}!') 
                else:
                    self.batch = results.get("data").get("updateBatch")
                    if self.batch:
                        return True
                    else:
                        self.log.error('Update batch failed!')
                        return False
            else:
                self.log.error(f'Update batch failed with status code: {status}.')
                return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Update batch failed! {get_exception_msg()}')
            return False


    

