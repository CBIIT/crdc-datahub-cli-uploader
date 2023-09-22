import argparse
import os
import yaml
from common.constants import UPLOAD_TYPE, UPLOAD_TYPES,INTENTION, INTENTIONS, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    API_URL, TOKEN, SUBMISSION_ID, FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD
from bento.common.utils import get_logger
from common.utils import clean_up_key_value


#requirements of the ticket CRDCDH-13:
UPLOAD_HELP = """
Command line arguments / configuration
--api-url, API endpoint URL, required
--token, API token string, required
--submission, submission ID, required
--type, valid value in [“file”, “metadata”], required
--data, folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required
--config, configuration file path, can potentially contain all above parameters, optional{}
Following arguments are needed to read important data from manifest, conditional required when type = “file”

--manifest, path to manifest file, conditional required when type = “file”
--name-field
--size-field
--md5-field
Following argument is needed when type = "metadata"

--intention, valid value in [“New”, “Update”, “Delete”], conditional required when type = “metadata”, default to “new”
CLI Argument and configuration module will

validate and combine parameters from CLI and/or config file
If config_file is given, then everything else is potentially optional (if it’s included in config file)
Some arguments are only needed for type = “file” or type = “metadata”, e.g., —intention, —manifest

"""

class Config():
    def __init__(self):
        self.log = get_logger('Upload Config')
        parser = argparse.ArgumentParser(description='Upload files to AWS s3 bucket')
        parser.add_argument('-a', '--api-url', help='API endpoint URL, required')
        parser.add_argument('-k', '--token', help='API token string, required')
        parser.add_argument('-u', '--submission', help='submission ID, required')
        parser.add_argument('-t', '--type', help='valid value in [“file”, “metadata”], required')
        parser.add_argument('-d', '--data', help='folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required')
        
        #args for file type
        parser.add_argument('-f', '--manifest', help='path to manifest file, conditional required when type = “file"')
        parser.add_argument('-n', '--name-field', help='header file name in manifest, optional, default value is "file_name"')
        parser.add_argument('-s', '--size-field', help='header file size in manifest, optional, default value is "file_size"')
        parser.add_argument('-m', '--md5-field', help='header file size nin manifest, optional, default value is "md5sum"')

        #args for metadata type
        parser.add_argument('-i', '--intention,', help='valid value in [“New”, “Update”, “Delete”], conditional required when type = “metadata”, default to “new”')

        #for better user experience, using configuration file to pass all args above
        parser.add_argument('-c', '--config', help='configuration file, can potentially contain all above parameters, optional')
        #parser.add_argument('config_file', help='configuration file, contain all parameters without input args one by one, preferred')
       
        args = parser.parse_args()
        self.data = {}
        if args.config and os.path.isfile(args.config.strip()):
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

    def validate(self):
        if len(self.data)== 0:
            return False
        self.data = clean_up_key_value(self.data)
        
        apiUrl = self.data.get(API_URL)
        if apiUrl is None:
            self.log.critical(f'api url is required!')
            return False
        else:
            self.data[API_URL]  = apiUrl

        token = self.data.get(TOKEN)
        if token is None:
            self.log.critical(f'token is required!')
            return False
        else:
            self.data[TOKEN]  = token
        
        submissionId = self.data.get(SUBMISSION_ID)
        if submissionId is None:
            self.log.critical(f'submission Id is required!')
            return False
        # elif isinstance(submissionId, str):  requirement changed, the submit id is a string now
        #     if not submissionId.isdigit():
        #         self.log.critical(f'submission Id is not integer!')
        #     else:
        #         self.data[SUBMISSION_ID] =int(submissionId) 
        
        type = self.data.get(UPLOAD_TYPE)
        if type is None:
            self.log.critical(f'upload type is required!')
            return False
        elif type not in UPLOAD_TYPES:
            self.log.critical(f'{type} is not valid uploading type!')
            return False
        else:
            self.data[UPLOAD_TYPE] = type
            if type == UPLOAD_TYPES[0]: #file
                #check manifest
                manifest = self.data.get(PRE_MANIFEST)
                if manifest is None:
                    self.log.critical(f'manifest file path is required for file uploading!')
                    return False
                if not os.path.isfile(manifest): 
                    self.log.critical(f'pre-manifest file path is not valid!')
                    return False
                
                self.data[PRE_MANIFEST]  = manifest
                #check header names in manifest file
                file_name_hearder= self.data.get(FILE_NAME_FIELD)
                if file_name_hearder is None:
                    self.data[FILE_NAME_FIELD] = FILE_NAME_DEFAULT

                file_size_hearder = self.data.get(FILE_SIZE_FIELD)
                if file_size_hearder is None:
                    self.data[FILE_SIZE_FIELD] = FILE_SIZE_DEFAULT

                md5_header = self.data.get(FILE_MD5_FIELD)
                if  md5_header is None:
                    self.data[FILE_MD5_FIELD] = MD5_DEFAULT

            elif type == UPLOAD_TYPES[1]: #metadata
                #check intention
                intention = self.data.get(INTENTION)
                if intention is None:
                    self.log.critical(f'intention is required for metadata uploading!')
                    return False
                elif intention not in INTENTIONS:
                    self.log.critical(f'{intention} is not a valid intention!')
                    return False
        
        filepath = self.data.get(FILE_DIR)
        if filepath is None:
            self.log.critical(f'data file path is required!')
            return False
        else:
            filepath = filepath
            self.data[FILE_DIR]  = filepath
            if not os.path.isdir(filepath): 
                self.log.critical(f'data file path is not valid!')
                return False
  
        return True

