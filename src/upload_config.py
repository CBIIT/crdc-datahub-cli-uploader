import argparse
import os
import yaml
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    API_URL, TOKEN, SUBMISSION_ID, FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD, RETRIES, OVERWRITE, \
    DRY_RUN, TYPE_FILE, FILE_ID_FIELD, OMIT_DCF_PREFIX, S3_START, FROM_S3, HEARTBEAT_INTERVAL_CONFIG, CLI_VERSION, ARCHIVE_MANIFEST
from bento.common.utils import get_logger
from common.graphql_client import APIInvoker
from common.utils import clean_up_key_value, compare_version
CLI_VERSION_API = "https://hub.datacommons.cancer.gov/api/graphql"
class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-v', '--version', action='store_true', help='Show version and continue')
        parser.add_argument('-a', '--api-url', help='API endpoint URL, required')
        parser.add_argument('-k', '--token', help='API token string, required')
        parser.add_argument('-u', '--submission', help='submission ID, required')
        parser.add_argument('-t', '--type', choices=UPLOAD_TYPES, help='valid value in [“data file”, “metadata”], required')
        parser.add_argument('-d', '--data', help='folder that contains either data files (type = “data file”) or metadata (TSV/TXT) files (type = “metadata”), required')
        parser.add_argument('--overwrite', default=False, type=bool, help='Overwrite file even same size file already exists at destination, optional, default is false')
        parser.add_argument('--dryrun', default=False, type=bool, help='Only check original file, won\'t copy any files, optional, default is false')
        #args for data file type
        parser.add_argument('-f', '--manifest', help='path to manifest file, conditional required when type = “data file"')

        parser.add_argument('-r', '--retries', type=int, help='file uploading retries, optional, default value is 3')

        #for better user experience, using configuration file to pass all args above
        parser.add_argument('-c', '--config', help='configuration file, can potentially contain all above parameters, optional')
        # Bypass archive(zip) validation, archive manifest is no longer required
        parser.add_argument('--bypass-archive-validation', action='store_true', default=False, help='Bypass archive(zip) validation, archive manifest is no longer required')
        
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

        retry = self.data.get(RETRIES) 
        if not retry:
            self.data[RETRIES] = 3 #default value is 3
        elif isinstance(retry, str):
            if not retry.isdigit():
                self.log.warning(f'Configuration warning in “retries”: “{retry}” is not a valid integer. It is set to 3.')
                self.data[RETRIES] = 3
            else:
                self.data[RETRIES] =int(retry) 
        else:
            self.data[RETRIES] =int(retry)

        overwrite = self.data.get(OVERWRITE, False) #default value is False
        if isinstance(overwrite, str):
            overwrite = True if overwrite.lower() == "true" else False
            self.data[OVERWRITE] = overwrite

        dry_run = self.data.get(DRY_RUN, False) #default value is False
        if isinstance(dry_run, str):
            dry_run = True if str(dry_run).lower() == "true" else False
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
                if not manifest.startswith(S3_START):
                    if not "/" in manifest or not "\\" in manifest:  # user only put file name but not path
                        manifest = os.path.join("./", manifest)
                    if not os.path.isfile(manifest): 
                        self.log.critical(f'Manifest file “{manifest}” is not readable. Please make sure the path is correct and the file is readable.')
                        return False

                self.data[PRE_MANIFEST]  = manifest
    
        filepath = self.data.get(FILE_DIR)
        if filepath is None:
            self.log.critical(f'Please provide “data” (path to data files) in configuration file or command line argument.')
            return False
        else:
            self.data[FILE_DIR]  = filepath
            if not filepath.startswith(S3_START):
                self.data[FROM_S3] = False
                if not os.path.isdir(filepath): 
                    self.log.critical(f'Configuration error in “data” (path to data files): “{filepath}” is not valid')
                    return False
            else:
                self.data[FROM_S3] = True
  
        return True
    
    def validate_file_config(self, data_file_config):
        #check header names in manifest file
        file_name_header= data_file_config.get(FILE_NAME_FIELD.replace("-", "_"))
        self.data[FILE_NAME_FIELD] = file_name_header if file_name_header else FILE_NAME_DEFAULT

        file_size_header = data_file_config.get(FILE_SIZE_FIELD.replace("-", "_"))
        self.data[FILE_SIZE_FIELD] = file_size_header if file_size_header else FILE_SIZE_DEFAULT

        md5_header = data_file_config.get(FILE_MD5_FIELD.replace("-", "_"))
        self.data[FILE_MD5_FIELD] = md5_header if md5_header else MD5_DEFAULT

        file_id_header= data_file_config.get(FILE_ID_FIELD.replace("-", "_"))
        if file_id_header is None:
            self.log.critical(f'file id field is required.')
            return False
        
        self.data[FILE_ID_FIELD] = file_id_header

        omit_dcf_prefix = data_file_config.get(OMIT_DCF_PREFIX.replace("-", "_"))
        self.data[OMIT_DCF_PREFIX] = False if omit_dcf_prefix is None else omit_dcf_prefix

        heartbeat_interval = data_file_config.get(HEARTBEAT_INTERVAL_CONFIG)
        self.data[HEARTBEAT_INTERVAL_CONFIG] = heartbeat_interval if heartbeat_interval else 300 #5min

        return True
    
    def check_version(self):
        configs = self.data if self.data.get(API_URL) else {API_URL: CLI_VERSION_API}
        apiInvoker = APIInvoker(configs)
        result, available_version = apiInvoker.get_cli_version()
        if not result:
            msg = f"Failed to check CLI version, can't retrieve configuration from API: {CLI_VERSION_API}"
            return -1, msg
        apiInvoker = None
        return compare_version(available_version, CLI_VERSION)



