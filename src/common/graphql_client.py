#!/usr/bin/env python3

import requests
import json
from bento.common.utils import get_logger
from common.constants import UPLOAD_TYPE, API_URL, SUBMISSION_ID, TOKEN
from common.utils import get_exception_msg

class APIInvoker:
    def __init__(self, configs):
        self.token = configs.get(TOKEN)
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.url = configs.get(API_URL)
        self.submissionId = configs.get(SUBMISSION_ID)
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
                    self.log.error(f'Retrieve temporary credential failed - {results.get("errors")[0].get("message")}.')  
                    return False
                else:
                    self.cred = results.get("data").get("createTempCredentials")
                    return True  
            else:
                self.log.error(f'Retrieve temporary credential failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return False

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Retrieve temporary credential failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False


    #2) create upload batch
    def create_batch(self, file_array):
        self.new_batch = None
        #adjust file list to match the graphql param.
        # file_array = json.dumps(file_array).replace("\"fileName\"", "fileName").replace("\"size\"", "size")
        file_array = json.dumps(file_array)
        body = f"""
        mutation {{
            createBatch (
                submissionID: \"{self.submissionId}\", 
                type: \"{self.type}\", 
                files: {file_array}
            ){{
                _id,
                submissionID,
                bucketName,
                filePrefix,
                type,
                fileCount,
                files {{
                    fileID, 
                    fileName,
                }}
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
                        self.log.error(f'Create batch failed: {results.get("errors")[0].get("message")}.') 
                else:
                    self.new_batch = results.get("data").get("createBatch")
                    if self.new_batch:
                        return True
                    else:
                        self.log.error('Create batch failed!')
                        return False
            else:
                self.log.error(f'Create batch failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return False
            
        except Exception as e:
            #self.log.debug(e)
            self.log.exception(f'Create batch failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False

    #3) update upload batch
    def update_batch(self, batchID, uploaded_files, uploading="false"):
        self.batch = None
        #adjust file list to match the graphql param.
        file_array = []
        if uploaded_files:
            file_array = json.dumps(uploaded_files).replace("\"fileName\"", "fileName").replace("\"succeeded\"", "succeeded").replace("\"errors\"", "errors").replace("\"skipped\"", "skipped") \
                if uploaded_files and len(uploaded_files) > 0 else json.dumps(uploaded_files)
        body = f"""
        mutation {{
            updateBatch (
                batchID: \"{batchID}\", 
                files: {file_array}, 
                uploading: {uploading}
                )
            {{
                _id,
                submissionID,
                type,
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
                        self.log.error(f'Update batch failed: {results.get("errors")[0].get("message")}.') 
                else:
                    self.batch = results.get("data").get("updateBatch")
                    if self.batch:
                        return True
                    else:
                        self.log.error('Update batch failed!')
                        return False
            else:
                self.log.error(f'Update batch failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return False
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Update batch failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False
        
    # 4) get_data_file_config()
    def get_data_file_config(self, submissionID):
        body = f"""
        query {{
            retrieveFileNodeConfig (submissionID: \"{submissionID}\") {{
                id_field,
                name_field,
                size_field,
                md5_field,
                omit_DCF_prefix, 
                heartbeat_interval
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info(f"get_data_file_config response status code: {status}.")
            if status == 200:
                results = response.json()
                if results.get("errors"):
                    msg = f'Get data file config failed: {results.get("errors")[0].get("message")}.'
                    self.log.error(msg)
                    return False, None
                else:
                    data_file_config = results.get("data").get("retrieveFileNodeConfig")
                    if data_file_config:
                        return True, data_file_config
                    else:
                        self.log.error('Get data file config failed!')
                        return False, None
            else:
                self.log.error(f'Get data file config failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return False, None

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Get data file config failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False, None


    

