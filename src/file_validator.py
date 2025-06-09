#!/usr/bin/env python3
import csv
import os
import glob
from common.constants import UPLOAD_TYPE, TYPE_FILE, TYPE_MATE_DATA, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_PATH, SUCCEEDED, ERRORS, FILE_ID_DEFAULT,\
    FILE_ID_FIELD, OMIT_DCF_PREFIX, FROM_S3, TEMP_DOWNLOAD_DIR, S3_START, MD5_CACHE_DIR, MD5_CACHE_FILE, MODIFIED_AT, SUBFOLDER_FILE_NAME
from bento.common.utils import get_logger
from common.utils import extract_s3_info_from_url, dump_data_to_csv, is_valid_uuid, clean_up_strs, clean_up_key_value
from common.s3util import S3Bucket
from common.md5_calculator import calculate_file_md5


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
        self.from_s3 = configs.get(FROM_S3)
        self.pre_manifest = configs.get(PRE_MANIFEST)
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('File_Validator')
        self.invalid_count = 0
        self.has_file_id = None
        self.manifest_rows = None
        self.field_names = None
        self.download_file_dir = None
        self.from_bucket_name = None
        self.from_prefix = None
        self.s3_bucket = None
        self.md5_cache_file = os.path.join(MD5_CACHE_DIR, MD5_CACHE_FILE)
        self.md5_cache = self.load_md5_cache() 

    def validate(self):
        # check file dir
        if not self.from_s3 and not os.path.isdir(self.file_dir):
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
            try: 
                return self.validate_size_md5()
            except Exception as e:
                self.log.critical(e)
                return False
            finally:
                if self.s3_bucket:
                    self.s3_bucket.close()
        
        else:
            self.log.critical(f'Invalid uploading type, {self.uploadType}!')
            return False
        return True

    #validate file's size and md5 against ree-manifest.   
    def validate_size_md5(self):
        self.files_info =  self.read_manifest()
        if not self.files_info or len(self.files_info ) == 0:
            return False
        if self.from_s3 == True:
            self.download_file_dir = TEMP_DOWNLOAD_DIR
            os.makedirs(self.download_file_dir, exist_ok=True)
            self.from_bucket_name, self.from_prefix = extract_s3_info_from_url(self.file_dir)
            self.s3_bucket = S3Bucket()
            self.s3_bucket.set_s3_client(self.from_bucket_name, None)
        line_num = 1
        total_file_cnt = len(self.files_info)
        self.log.info(f'Start to validate data files...')
        for info in self.files_info:
            line_num += 1
            invalid_reason = ""
            file_path = os.path.join(self.file_dir if not self.from_s3 else self.download_file_dir, info[FILE_NAME_DEFAULT])
            size = info.get(FILE_SIZE_DEFAULT)
            size_info = 0 if not size or not size.isdigit() else int(size)
            info[FILE_SIZE_DEFAULT]  = size_info #convert to int
            file_id = info.get(FILE_ID_DEFAULT)
            # validate file name
            result, msg = self.validate_file_name(info.get(FILE_NAME_DEFAULT), line_num, info)
            if not result:
                invalid_reason += msg
                self.fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: [invalid_reason]})
                self.invalid_count += 1
                self.log.error(invalid_reason)
                continue
            if not self.from_s3: # only validate local data file
                result = validate_data_file(info, file_id, size_info, file_path, self.fileList, self.md5_cache, invalid_reason, self.log)
                if result:
                    self.log.info(f'Validating file integrity succeeded on "{info[FILE_NAME_DEFAULT]}"')
                self.log.info(f'{line_num - 1} out of {total_file_cnt} file(s) have been validated.')
                if not result:
                    self.invalid_count += 1
                    continue
            else: # check file existing and validate file size in s3 bucket
                s3_file_size, msg = self.s3_bucket.get_object_size(os.path.join(self.from_prefix, info[FILE_NAME_DEFAULT]))
                if not s3_file_size:
                    invalid_reason += msg
                    self.fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: [invalid_reason]})
                    self.invalid_count += 1
                    self.log.error(invalid_reason)
                    continue
                if s3_file_size != size_info:
                    invalid_reason += f"Real file size {s3_file_size} of file {info[FILE_NAME_DEFAULT]} does not match with that in manifest {size_info}!"
                    self.fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: [invalid_reason]})
                    self.invalid_count += 1
                    self.log.error(invalid_reason)
                    continue
            file_size = size_info
            md5sum = info[MD5_DEFAULT]
            # validate file id
            result, msg = self.validate_file_id(file_id, line_num)
            if not result:
                invalid_reason += msg
                self.fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, SUCCEEDED: False, ERRORS: [invalid_reason]})
                self.invalid_count += 1
                self.log.error(invalid_reason)
                continue

            self.fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, SUCCEEDED: None, ERRORS: None, SUBFOLDER_FILE_NAME: info.get(SUBFOLDER_FILE_NAME)})
        # save md5 cache to file
        if not self.from_s3:
            dump_data_to_csv(self.md5_cache, self.md5_cache_file)
        "Validating file integrity succeeded"
        return True
    
    #public function to read pre-manifest and return list of file records 
    def read_manifest(self):
        files_info = []
        files_dict = {}
        manifest_rows = []
        is_s3_manifest = self.pre_manifest.startswith(S3_START)
        if is_s3_manifest:
            s3_bucket = None
            bucket_name, key = extract_s3_info_from_url(self.pre_manifest)
            self.download_file_dir = TEMP_DOWNLOAD_DIR
            os.makedirs(self.download_file_dir, exist_ok=True)
            local_manifest = os.path.join(self.download_file_dir, key.split('/')[-1])
            try:
                s3_bucket = S3Bucket()
                s3_bucket.set_s3_client(bucket_name, None)
                result, msg = s3_bucket.file_exists_on_s3(key)
                if  result == False:
                    self.log.critical(msg)
                    return None
                self.log.info(f'Downloading remote manifest file, "{self.pre_manifest}" ...')
                s3_bucket.download_object(key, local_manifest)
                self.log.info(f'Downloaded remote manifest file, "{self.pre_manifest}" successfully.')
                self.pre_manifest = self.configs[PRE_MANIFEST] = local_manifest
            except Exception as e:
                self.log.debug(e)
                self.log.exception(f"Downloading manifest failed - internal error. Please try again and contact the helpdesk if this error persists.")
                return None
            finally:
                if s3_bucket:
                    s3_bucket.close()

        try:
            with open(self.pre_manifest) as pre_m:
                reader = csv.DictReader(pre_m, delimiter='\t')
                if not self.field_names:
                    self.field_names = clean_up_strs(reader.fieldnames)
                for info in reader:
                    file_info = clean_up_key_value(info)
                    manifest_rows.append(file_info)
                    file_name = file_info[self.configs.get(FILE_NAME_FIELD)]
                    file_id = file_info.get(self.configs.get(FILE_ID_FIELD))
                    if self.has_file_id is None:
                        self.has_file_id = self.configs.get(FILE_ID_FIELD) in info.keys()
                    files_dict.update({file_name: {
                        FILE_ID_DEFAULT: file_id,
                        FILE_NAME_DEFAULT: file_name,
                        FILE_SIZE_DEFAULT: file_info[self.configs.get(FILE_SIZE_FIELD)],
                        MD5_DEFAULT: file_info[self.configs.get(FILE_MD5_FIELD)]
                    }})
            files_info  =  list(files_dict.values())
            self.manifest_rows = manifest_rows
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Reading manifest failed - internal error. Please try again and contact the helpdesk if this error persists.")
        return files_info
    """
    validate file id format
    return: True or False, error message
    """
    def validate_file_id(self, id, line_num):
        id_field_name = self.configs.get(FILE_ID_FIELD)
        if id:
            if self.configs[OMIT_DCF_PREFIX] == False:
                msg = f'Line {line_num}: "{id_field_name}": "{id}" is not in correct format. A correct "{id_field_name}" should look like "dg.4DFC/e041576e-3595-5c8b-b0b3-272bc7cb6aa8". You can provide correct "{id_field_name}" or remove the column and let the system generate it for you.'
                if not id.startswith("dg.4DFC/"):
                    # self.log.error(msg)
                    return False, msg
                else:
                    uuid = id.split('/')[1]
                    if not is_valid_uuid(uuid):
                        self.log.error(msg)
                        return False, msg  
            else:
                if(not is_valid_uuid(id)):
                    msg = f'Line {line_num}: "{id_field_name}": "{id}" is not in correct format. A correct "{id_field_name}" should look like "e041576e-3595-5c8b-b0b3-272bc7cb6aa8". You can provide correct "{id_field_name}" or remove the column and let the system generate it for you.'
                    # self.log.error(msg)
                    return False, msg 
        else:
            if self.has_file_id:
                msg = f'Line {line_num}: "{id_field_name}" is required but not provided. You can provide correct "{id_field_name}" or remove the column and let the system generate it for you.'
                # self.log.error(msg)
                return False, msg
                
        return True, None
    
    def validate_file_name(self, file_name, line_num, file_info):
        if not file_name:
            msg = f'Line {line_num}: File name is required but not provided. Please provide a valid file name.'
            self.log.error(msg)
            return False, msg
        # check file name is a absolute path
        if os.path.isabs(file_name):
            msg = f'Line {line_num}: File name "{file_name}" is not a valid file name. Please provide a valid file name.'
            self.log.error(msg)
            return False, msg
        # check if file name contains / and or \ set internal_file_name to file_info with file_name with replace by _
        if '/' in file_name or '\\' in file_name:
            file_info[SUBFOLDER_FILE_NAME] = file_name.replace('/', '_').replace('\\', '_')
        return True, None
    
    def load_md5_cache(self):
        # retrieve cached md5 info
        # check if md5 cache dir exists
        os.makedirs(MD5_CACHE_DIR, exist_ok=True)
        if os.path.isfile(self.md5_cache_file):
            # read md5 cache file to dict
            with open(self.md5_cache_file) as f:
                reader = csv.DictReader(f)
                return [row for row in reader]  
        else: 
            return []
    

"""
Validate file size and md5
:param file_info: file_info
:param size_info: size_info
:param file_path: file_path
:param fileList: fileList
:param md5_cache: md5_cache
:param invalid_reason: invalid_reason
:param log: log
:return: True if valid, False otherwise
"""
def validate_data_file(file_info, file_id, size_info, file_path, fileList, md5_cache, invalid_reason, log):
    if not os.path.isfile(file_path):
        invalid_reason += f"File {file_path} does not exist!"
        fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: file_info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: file_info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: [invalid_reason]})
        log.error(invalid_reason)
        return False
    file_size = os.path.getsize(file_path)
    if file_size != size_info:
        invalid_reason += f"Real file size {file_size} of file {file_info[FILE_NAME_DEFAULT]} does not match with that in manifest {file_info[FILE_SIZE_DEFAULT]}!"
        fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: file_info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: file_info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: invalid_reason})
        log.error(invalid_reason)
        return False
    md5_info = file_info[MD5_DEFAULT] 
    if not md5_info:
        invalid_reason += f"MD5 of {file_info[FILE_NAME_DEFAULT]} is not set in the pre-manifest!"
        fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: file_info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path,  FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: file_info[MD5_DEFAULT], SUCCEEDED: False, ERRORS: [invalid_reason]})
        log.error(invalid_reason)
        return False
    md5sum = get_file_md5(file_path, md5_cache, file_size, log)
    if md5_info != md5sum:
        invalid_reason += f"Real file md5 {md5sum} of file {file_info[FILE_NAME_DEFAULT]} does not match with that in manifest {md5_info}!"
        fileList.append({FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: file_info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, SUCCEEDED: False, ERRORS: [invalid_reason]})
        log.error(invalid_reason)
        return False
    return True

def get_file_md5(file_path, md5_cache, file_size, log):
    """
    retrieve md5 if existing cached value, otherwise calculate md5 for the file and save to md5 cache
    """
    file_modified_at = os.path.getmtime(file_path)
    # check if md5 is in cache by file name and file size
    cached_md5 = [row[MD5_DEFAULT] for row in md5_cache if row[FILE_PATH] == file_path and row[FILE_SIZE_DEFAULT] == str(file_size) and 
                    row[MODIFIED_AT] == str(file_modified_at)]
    if not cached_md5 or len(cached_md5) == 0:
         #calculate file md5
        md5sum = calculate_file_md5(file_path, file_size, log)
        md5_cache.append({FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, MODIFIED_AT: file_modified_at})
    else:
        md5sum = cached_md5[0]

    return md5sum
