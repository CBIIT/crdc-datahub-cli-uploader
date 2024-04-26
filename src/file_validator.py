#!/usr/bin/env python3

import csv
import os
import glob

from common.constants import UPLOAD_TYPE, TYPE_FILE, TYPE_MATE_DATA, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
     FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_PATH, SUCCEEDED, ERRORS
from common.utils import clean_up_key_value, clean_up_strs, get_exception_msg
from bento.common.utils import get_logger, get_md5


""" Requirement for the ticket crdcdh-343
For files: read manifest file and validate local files’ sizes and md5s
For metadata: validate data folder contains TSV or TXT files
Compose a list of files to be updated and their sizes (metadata or files)
"""

class FileValidator:
    
    def __init__(self, configs):
        self.configs = configs
        self.uploadType = configs.get(UPLOAD_TYPE)
        self.file_dir = configs.get(FILE_DIR)
        self.pre_manifest = configs.get(PRE_MANIFEST)
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('File_Validator')
        self.invalid_count = 0

    def validate(self):
        # check file dir
        if not os.path.isdir(self.file_dir):
            self.log.critical(f'data file path is not valid!')
            return False
        
        
        if self.uploadType == TYPE_MATE_DATA: #metadata
            txt_files = glob.glob('{}/*.txt'.format(self.file_dir ))
            tsv_files = glob.glob('{}/*.tsv'.format(self.file_dir ))
            file_list = txt_files + tsv_files
            if len(file_list) == 0:
                self.log.critical(f'No valid metadata file(s) found!')
                return False
            
            for filepath in file_list:
                size = os.path.getsize(filepath)
                filename = os.path.basename(filepath)
                #metadata file dictionary: {FILE_NAME_DEFAULT: None, FILE_SIZE_DEFAULT: None}
                self.fileList.append({FILE_NAME_DEFAULT:filename, FILE_PATH: filepath, FILE_SIZE_DEFAULT: size})

        elif self.uploadType == TYPE_FILE: #file
            if not os.path.isfile(self.pre_manifest):
                self.log.critical(f'manifest file is not valid!')
                return False
            return self.validate_size_md5()
        
        else:
            self.log.critical(f'Invalid uploading type, {self.uploadType}!')
            return False
        return True

    #validate file's size and md5 against ree-manifest.   
    def validate_size_md5(self):
        self.files_info =  self.read_manifest()
        if not self.files_info or len(self.files_info ) == 0:
            return False
        
        for info in self.files_info:
            invalid_reason = ""
            file_path = os.path.join(self.file_dir, info[FILE_NAME_DEFAULT])
            size = info.get(FILE_SIZE_DEFAULT)
            size_info = 0 if not size or not size.isdigit() else int(size)
            info[FILE_SIZE_DEFAULT]  = size_info #convert to int

            if not os.path.isfile(file_path):
                invalid_reason += f"File {file_path} does not exist!"
                #file dictionary: {FILE_NAME_DEFAULT: None, FILE_SIZE_DEFAULT: None, FILE_INVALID_REASON: None}
                self.fileList.append({FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: None, SUCCEEDED: False, ERRORS: [invalid_reason]})
                self.invalid_count += 1
                continue
            
            file_size = os.path.getsize(file_path)
            if file_size != size_info:
                invalid_reason += f"Real file size {file_size} of file {info[FILE_NAME_DEFAULT]} does not match with that in manifest {info[FILE_SIZE_DEFAULT]}!"
                self.fileList.append({FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: None, SUCCEEDED: False, ERRORS: invalid_reason})
                self.invalid_count += 1
                continue

            md5_info = info[MD5_DEFAULT] 
            if not md5_info:
                invalid_reason += f"MD5 of {info[FILE_NAME_DEFAULT]} is not set in the pre-manifest!"
                self.fileList.append({FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path,  FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: None, SUCCEEDED: False, ERRORS: [invalid_reason]})
                self.invalid_count += 1
                continue
            #calculate file md5
            md5sum = get_md5(file_path)
            if md5_info != md5sum:
                invalid_reason += f"Real file md5 {md5sum} of file {info[FILE_NAME_DEFAULT]} does not match with that in manifest {md5_info}!"
                self.fileList.append({FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, SUCCEEDED: False, ERRORS: [invalid_reason]})
                self.invalid_count += 1
                continue

            self.fileList.append({FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, SUCCEEDED: None, ERRORS: None})

        return True
    
    #public function to read pre-manifest and return list of file records 
    def read_manifest(self):
        files_info = []
        files_dict = {}
        try:
            with open(self.pre_manifest) as pre_m:
                reader = csv.DictReader(pre_m, delimiter='\t')
                self.field_names = clean_up_strs(reader.fieldnames)
                for info in reader:
                    file_info = clean_up_key_value(info)
                    file_name = file_info[self.configs.get(FILE_NAME_FIELD)]
                    files_dict.update({file_name: {
                        FILE_NAME_DEFAULT: file_name,
                        FILE_SIZE_DEFAULT: file_info[self.configs.get(FILE_SIZE_FIELD)],
                        MD5_DEFAULT: file_info[self.configs.get(FILE_MD5_FIELD)]
                    }})
            files_info  =  list(files_dict.values())
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Failed to read pre-manifest file! {get_exception_msg}")
        return files_info

