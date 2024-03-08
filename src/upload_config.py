import argparse
import os
import yaml
import sys
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES,INTENTION, INTENTIONS, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    API_URL, TOKEN, SUBMISSION_ID, FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD, RETRIES, OVERWRITE, \
        DRY_RUN, TYPE_FILE, TYPE_MATE_DATA, INTENTION_NEW
from bento.common.utils import get_logger
from common.utils import clean_up_key_value

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-a', '--api-url', help='API endpoint URL, required')
        parser.add_argument('-k', '--token', help='API token string, required')
        parser.add_argument('-u', '--submission', help='submission ID, required')
        parser.add_argument('-t', '--type', choices=UPLOAD_TYPES, help='valid value in [“data file”, “metadata”], required')
        parser.add_argument('-d', '--data', help='folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required')
        parser.add_argument('--overwrite', default=False, type=bool, help='Overwrite file even same size file already exists at destination, optional, default is false')
        parser.add_argument('--dryrun', default=False, type=bool, help='Only check original file, won\'t copy any files, optional, default is false')
        #args for file type
        parser.add_argument('-f', '--manifest', help='path to manifest file, conditional required when type = “file"')
        parser.add_argument('-n', '--name-field', help='header file name in manifest, optional, default value is "file_name"')
        parser.add_argument('-s', '--size-field', help='header file size in manifest, optional, default value is "file_size"')
        parser.add_argument('-m', '--md5-field', choices=INTENTIONS ,help='header file size nin manifest, optional, default value is "md5sum"')
        parser.add_argument('-r', '--retries', default=3, type=int, help='file uploading retries, optional, default value is 3')
        #args for metadata type
        parser.add_argument('-i', '--intention,', help='valid value in [“New”, “Update”, “Delete”], conditional required when type = “metadata”, default to “new”')

        #for better user experience, using configuration file to pass all args above
        parser.add_argument('-c', '--config', help='configuration file, can potentially contain all above parameters, optional')
       
        args = parser.parse_args()
        self.data = {}

        if args.config:
            if not os.path.isfile(args.config.strip()):
                self.log.critical(f'Configuration file “{args.config}” is not readable. Please make sure the path is correct and the file is readable.')
                return None
            with open(args.config.strip()) as c_file:
                self.data = yaml.safe_load(c_file)['Config']

        self._override(args)

    def _override(self, args):
        for key, value in vars(args).items():
            # Ignore config file argument
            if key == 'config':
                continue
            if isinstance(value, bool):
                if value:
                    self.data[key] = value
            elif value is not None:
                self.data[key] = value
    # although some args are validated, 
    # we still need to validate all in case user only uses config file
    def validate(self):
        if len(self.data)== 0:
            return False
        self.data = clean_up_key_value(self.data)
        
        apiUrl = self.data.get(API_URL)
        if apiUrl is None:
            self.log.critical(f'Please provide “api_url” in configuration file or command line argument.')
            return False

        token = self.data.get(TOKEN)
        if token is None:
            self.log.critical(f'Please provide “token” in configuration file or command line argument.')
            return False

        
        submissionId = self.data.get(SUBMISSION_ID)
        if submissionId is None:
            self.log.critical(f'Please provide “submission” (submission ID) in configuration file or command line argument.')
            return False

        retry = self.data.get(RETRIES, 3) #default value is 3
        if isinstance(retry, str):
            if not retry.isdigit():
                self.log.critical(f'Configuration error in “retries”: “{retry}” is not a valid integer.')
                return False
            else:
                self.data[retry] =int(retry) 
        else:
            self.data[RETRIES] =int(retry) 

        overwrite = self.data.get(OVERWRITE, False) #default value is False
        if isinstance(overwrite, str):
            overwrite = True if overwrite.lower() == "true" else False
            self.data[OVERWRITE] = overwrite

        dry_run = self.data.get(DRY_RUN, False) #default value is False
        if isinstance(dry_run, str):
            dry_run = True if overwrite.lower() == "true" else False
            self.data[DRY_RUN] = dry_run

        type = self.data.get(UPLOAD_TYPE)
        if type is None:
            self.log.critical(f'Please provide “type” (“metadata” or “data file”) in configuration file or command line argument.')
            return False
        elif type not in UPLOAD_TYPES:
            self.log.critical(f'Configuration error in "type": “{type}” is not valid. Valid “type” value can be one of [“data file”, “metadata”]')
            return False
        else:
            if type == TYPE_FILE: #data file
                #check manifest
                manifest = self.data.get(PRE_MANIFEST)
                if manifest is None:
                    self.log.critical(f'Please provide “manifest” in configuration file or command line argument.')
                    return False
                if not os.path.isfile(manifest): 
                    self.log.critical(f'Manifest file “{manifest}” is not readable. Please make sure the path is correct and the file is readable.')
                    return False
                
                self.data[PRE_MANIFEST]  = manifest
                #check header names in manifest file
                file_name_header= self.data.get(FILE_NAME_FIELD)
                if file_name_header is None:
                    self.data[FILE_NAME_FIELD] = FILE_NAME_DEFAULT

                file_size_header = self.data.get(FILE_SIZE_FIELD)
                if file_size_header is None:
                    self.data[FILE_SIZE_FIELD] = FILE_SIZE_DEFAULT

                md5_header = self.data.get(FILE_MD5_FIELD)
                if  md5_header is None:
                    self.data[FILE_MD5_FIELD] = MD5_DEFAULT

            elif type == TYPE_MATE_DATA: #metadata
                #check intention
                intention = self.data.get(INTENTION)
                if intention is None:
                    self.log.critical(f'Please provide “intention” in configuration file or command line argument. Valid “intention” value can be one of [“New”, “Update”, “Delete”]')
                    return False
                elif intention not in INTENTIONS:
                    self.log.critical(f'Configuration error in “intention”: “{intention}” is not valid. Valid “intention” value can be one of [“New”, “Update”, “Delete”]')
                    return False
        
        filepath = self.data.get(FILE_DIR)
        if filepath is None:
            self.log.critical(f'Please provide “data” (path to data files) in configuration file or command line argument.')
            return False
        else:
            self.data[FILE_DIR]  = filepath
            if not os.path.isdir(filepath): 
                self.log.critical(f'Configuration error in “data” (path to data files): “{filepath}” is not valid')
                return False
  
        return True

