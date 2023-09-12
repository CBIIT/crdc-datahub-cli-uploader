#!/usr/bin/env python3

import requests
import json

class gql_api_invoker:
    def __init__(self, api_token):
        self.token = api_token
        self.headers = {'Authorization': f'Bearer {self.token}'}

    #to do 
    #1) get sts temp creadential for file/metadata uploading to S3 bucket

    #2) create upload batch

    #3) update upload batch

    

