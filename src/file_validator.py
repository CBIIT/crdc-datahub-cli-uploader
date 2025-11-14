#!/usr/bin/env python3
import csv
import os
import glob
import re
import zipfile
import shutil
from common.constants import UPLOAD_TYPE, TYPE_FILE, TYPE_MATE_DATA, FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, \
    FILE_DIR, FILE_MD5_FIELD, PRE_MANIFEST, FILE_NAME_FIELD, FILE_SIZE_FIELD, FILE_PATH, SUCCEEDED, ERRORS, FILE_ID_DEFAULT,\
    FILE_ID_FIELD, OMIT_DCF_PREFIX, FROM_S3, TEMP_DOWNLOAD_DIR, S3_START, MD5_CACHE_DIR, MD5_CACHE_FILE, MODIFIED_AT, SUBFOLDER_FILE_NAME,\
    TEMP_UNZIP_DIR, ARCHIVE_MANIFEST, ARCHIVE_NAME, MAX_CREATE_BATCH_PAYLOAD_SIZE, SUBMISSION_ID, BYPASS_ARCHIVE_VALIDATION
from common.utils import clean_up_key_value, clean_up_strs, is_valid_uuid
from bento.common.utils import get_logger
from common.utils import extract_s3_info_from_url, dump_data_to_csv
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
        self.archive_manifest= configs.get(ARCHIVE_MANIFEST)
        self.fileList = [] #list of files object {file_name, file_path, file_size, invalid_reason}
        self.log = get_logger('File_Validator')
        self.invalid_count = 0
        self.has_file_id = None
        self.manifest_rows = None
        self.archive_manifest_rows = None
        self.field_names = None
        self.download_file_dir = None
        self.from_bucket_name = None
        self.from_prefix = None
        self.s3_bucket = None
        self.md5_cache_file = os.path.join(MD5_CACHE_DIR, MD5_CACHE_FILE)
        self.md5_cache = self.load_md5_cache() 
        self.archive_files_info = []

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
        self.files_info, self.manifest_rows =  self.read_manifest()
        if not self.files_info or not self.manifest_rows:
            return False
        if self.archive_manifest:
            self.archive_files_info, self.archive_manifest_rows =  self.read_manifest(is_archive_manifest=True)
        if self.from_s3 == True:
            self.download_file_dir = TEMP_DOWNLOAD_DIR
            os.makedirs(self.download_file_dir, exist_ok=True)
            self.from_bucket_name, self.from_prefix = extract_s3_info_from_url(self.file_dir)
            self.s3_bucket = S3Bucket()
            self.s3_bucket.set_s3_client(self.from_bucket_name, None)
        line_num = 1
        total_file_cnt = len(self.files_info)
        result = check_payload_size(self.files_info, self.configs, self.log)
        if not result:
            return False
        self.log.info(f'Start to validate data files...')
        # add warning if manifest include "internal_file_name" column
        if SUBFOLDER_FILE_NAME in self.field_names:
            msg = "internal_file_name column found in the manifest! Values in internal_file_name column will be replaced by system generated values"
            self.log.warning(msg)
        self.field_names.append(SUBFOLDER_FILE_NAME) # add subfolder file name to field names
        # validate file name
        if not self.validate_file_name():
            return False
        self.field_names.append(SUBFOLDER_FILE_NAME) # add subfolder file name to field names
        for info in self.files_info:
            line_num += 1
            invalid_reason = ""
            file_name = info.get(FILE_NAME_DEFAULT)
            if '/' in file_name or '\\' in file_name:
                info[SUBFOLDER_FILE_NAME] = file_name.replace('/', '_').replace('\\', '_')
            else:
                info[SUBFOLDER_FILE_NAME] = file_name
            file_path = os.path.join(self.file_dir if not self.from_s3 else self.download_file_dir, file_name)
            size = info.get(FILE_SIZE_DEFAULT)
            if not size:
                invalid_reason += f"File size is missing for file {file_name}!"
                self.log.error(invalid_reason)
                self.invalid_count += 1
                continue
            size = str(size).replace(',', '')
            size_info = 0 if not size.isdigit() else int(size)
            info[FILE_SIZE_DEFAULT]  = size_info #convert to int
            file_id = info.get(FILE_ID_DEFAULT)
            converted_file_info = {FILE_ID_DEFAULT: file_id, FILE_NAME_DEFAULT: info.get(FILE_NAME_DEFAULT), FILE_PATH: file_path, FILE_SIZE_DEFAULT: size_info, MD5_DEFAULT: info[MD5_DEFAULT], SUCCEEDED: None, ERRORS: None, SUBFOLDER_FILE_NAME: info.get(SUBFOLDER_FILE_NAME)}
            self.fileList.append(converted_file_info)
            if not self.from_s3: # only  validate local data file
                result = validate_data_file(converted_file_info, size_info, file_path, self.md5_cache, self.log, self.archive_files_info, self.configs.get(BYPASS_ARCHIVE_VALIDATION, False))
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
                    converted_file_info[SUCCEEDED] = False
                    converted_file_info[ERRORS] = [invalid_reason]
                    self.invalid_count += 1
                    self.log.error(invalid_reason)
                    continue
                if s3_file_size != size_info:
                    invalid_reason += f"Real file size {s3_file_size} of file {info[FILE_NAME_DEFAULT]} does not match with that in manifest {size_info}!"
                    converted_file_info[SUCCEEDED] = False
                    converted_file_info[ERRORS] = [invalid_reason]
                    self.invalid_count += 1
                    self.log.error(invalid_reason)
                    continue
            # validate file id
            result, msg = self.validate_file_id(file_id, line_num)
            if not result:
                invalid_reason += msg
                converted_file_info[SUCCEEDED] = False
                converted_file_info[ERRORS] = [invalid_reason]
                self.invalid_count += 1
                self.log.error(invalid_reason)
                continue

        # save md5 cache to file
        if not self.from_s3:
            dump_data_to_csv(self.md5_cache, self.md5_cache_file)
        return True
    
    # validate file name listed manifest
    def validate_file_name(self):
        msg = None
        is_valid = True
        line_num = 2
        file_name_config = self.configs.get(FILE_NAME_FIELD)
        md5_config = self.configs.get(FILE_MD5_FIELD)
        self.log.info("Start validating file names listed in pre-manifest:")
        for row in self.manifest_rows:
            file_name = row[file_name_config]
            if not file_name or not file_name.strip():
                msg = f"Line {line_num}: File name is empty!"
                is_valid = False
                self.log.error(msg)
            md5 = row[md5_config]
            # check if file name is unique by count the file name
            same_file_name_list  = [file for file in self.manifest_rows if file[file_name_config] == file_name and file[md5_config] != md5]
            if len(same_file_name_list) > 0:
                msg = f"Line {line_num}: File name {file_name} is not unique in the manifest!"
                is_valid = False
                self.log.error(msg)

            # check file name is a absolute path
            if os.path.isabs(file_name):
                msg = f'Line {line_num}: File name "{file_name}" is invalid, no absolute path allowed!'
                is_valid = False
                self.log.error(msg)

            # check if file name contains reserved or illegal characters, /, \, :, *, ?, ", <, >, and |
            if re.search(r'[*|]', file_name):
                msg = f"Line {line_num}: File name {file_name} contains invalid characters!"
                is_valid = False
                self.log.error(msg)

            line_num += 1
        self.log.info("Completed validating file names listed in pre-manifest.")
        return is_valid

    #public function to read pre-manifest and return list of file records 
    def read_manifest(self, is_archive_manifest=False):
        files_info = []
        files_dict = {}
        manifest_rows = []
        pre_manifest = self.pre_manifest if not is_archive_manifest else self.archive_manifest
        is_s3_manifest = pre_manifest.startswith(S3_START)
        if is_s3_manifest:
            bucket_name, key = extract_s3_info_from_url(pre_manifest)
            self.download_file_dir = TEMP_DOWNLOAD_DIR
            os.makedirs(self.download_file_dir, exist_ok=True)
            local_manifest = os.path.join(self.download_file_dir, key.split('/')[-1])
            result = self.download_s3_manifest(bucket_name, key, local_manifest)
            if not result:
                return [], []
            if not is_archive_manifest:
                self.pre_manifest = pre_manifest = self.configs[PRE_MANIFEST] = local_manifest
            else:
                self.archive_manifest = pre_manifest = self.configs[ARCHIVE_MANIFEST] = local_manifest 

        try:
            if not os.path.isfile(pre_manifest):
                self.log.critical(f'Manifest file {pre_manifest} does not exist!')
                return [], []
            with open(pre_manifest, mode='r', encoding='utf-8') as pre_m:
                reader = csv.DictReader(pre_m, delimiter='\t')
                if not self.field_names:
                    self.field_names = clean_up_strs(reader.fieldnames)
                for info in reader:
                    file_info = clean_up_key_value(info)
                    if not is_archive_manifest:
                        # convert MD5 to lowercase
                        file_info[self.configs.get(FILE_MD5_FIELD)] = file_info.get(self.configs.get(FILE_MD5_FIELD), "").lower()
                        file_name = file_info.get(self.configs.get(FILE_NAME_FIELD))
                        file_id = file_info.get(self.configs.get(FILE_ID_FIELD))
                        if self.has_file_id is None:
                            self.has_file_id = self.configs.get(FILE_ID_FIELD) in info.keys()
                        files_dict.update({file_name: {
                            FILE_ID_DEFAULT: file_id,
                            FILE_NAME_DEFAULT: file_name,
                            FILE_SIZE_DEFAULT: file_info.get(self.configs.get(FILE_SIZE_FIELD)),
                            MD5_DEFAULT: file_info.get(self.configs.get(FILE_MD5_FIELD))
                        }})
                    else:
                        # convert MD5 to lowercase
                        file_info["md5"] = file_info.get("md5", "").lower()
                        archive_file_name = file_info.get(ARCHIVE_NAME)
                        file_path = file_info.get(FILE_PATH)
                        key = f"{archive_file_name}-{file_path}"
                        files_dict.update({key: {
                            ARCHIVE_NAME: archive_file_name,
                            FILE_PATH: file_path,
                            FILE_SIZE_DEFAULT: file_info.get(FILE_SIZE_DEFAULT),
                            MD5_DEFAULT: file_info.get("md5")
                        }})
                    # save clean data to manifest_rows
                    manifest_rows.append(file_info)
            files_info  =  list(files_dict.values())

        except UnicodeDecodeError as ue:
            # self.log.debug(ue)
            self.log.exception(f"Reading manifest failed - manifest file contains non-ASCII characters.")
            return [], []
        except Exception as e:
            # self.log.debug(e)
            self.log.exception(f"Reading manifest failed - internal error. Please try again and contact the helpdesk if this error persists.")
            return [], []
        return files_info, manifest_rows
    
    def download_s3_manifest(self, bucket_name, key, local_manifest):
        s3_bucket = None
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
            return True
        except Exception as e:
            self.log.debug(e)
            self.log.exception(f"Downloading manifest failed - internal error. Please try again and contact the helpdesk if this error persists.")
            return False
        finally:
            if s3_bucket:
                s3_bucket.close()
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
def validate_data_file(file_info, size_info, file_path, md5_cache, log, archived_files_info = None, bypass_archive_validation = False):
    invalid_reason = ""
    if not os.path.isfile(file_path):
        invalid_reason += f"File {file_path} does not exist!"
        file_info[SUCCEEDED] = False
        file_info[ERRORS] = [invalid_reason]
        log.error(invalid_reason)
        return False
    file_size = os.path.getsize(file_path)
    if file_size != size_info:
        invalid_reason += f"Real file size {file_size} of file {file_info[FILE_NAME_DEFAULT]} does not match with that in manifest {file_info[FILE_SIZE_DEFAULT]}!"
        file_info[SUCCEEDED] = False
        file_info[ERRORS] = [invalid_reason]
        log.error(invalid_reason)
        return False
    md5_info = file_info[MD5_DEFAULT] 
    if not md5_info:
        invalid_reason += f"MD5 of {file_info[FILE_NAME_DEFAULT]} is not set in the pre-manifest!"
        file_info[SUCCEEDED] = False
        file_info[ERRORS] = [invalid_reason]
        log.error(invalid_reason)
        return False
    md5sum = get_file_md5(file_path, md5_cache, file_size, log)
    if md5_info != md5sum:
        invalid_reason += f"Real file md5 {md5sum} of file {file_info[FILE_NAME_DEFAULT]} does not match with that in manifest {md5_info}!"
        file_info[SUCCEEDED] = False
        file_info[ERRORS] = [invalid_reason]
        log.error(invalid_reason)
        return False
    # check zip file
    file_name = file_info.get(FILE_NAME_DEFAULT)
    if file_name.endswith('.zip') and bypass_archive_validation == False:
        log.info(f"Validating contents of zip file {file_name} ...")
        if not archived_files_info:
            invalid_reason += f"No archive manifest found for {file_name}, content of the zip archive cannot be validated."
            file_info[SUCCEEDED] = False
            file_info[ERRORS] = [invalid_reason]
            log.error(invalid_reason)
            return False
        archive_file_info_list = [row for row in archived_files_info if row.get(ARCHIVE_NAME) == file_name]
        if not archive_file_info_list or len(archive_file_info_list) == 0:
            invalid_reason += f"No archive manifest found for {file_name}, content of the zip archive cannot be validated."
            file_info[SUCCEEDED] = False
            file_info[ERRORS] = [invalid_reason]
            log.error(invalid_reason)
            return False
        if not validate_zip_file(archive_file_info_list, file_path, md5_cache, log):
            log.error(f"Failed validating contents of zip file {file_name}.")
            return False
        else:
            log.info(f"Validated contents of zip file {file_name} successfully.")
    return True

def validate_zip_file(archived_files_info, file_path, md5_cache, log):
    """
    validate zip file to unzip a file and validate size and md5 of each file in the zip against a separate archive manifest
    """
    try:
        # create temp dir for unzip and validate if not existing
        os.makedirs(TEMP_UNZIP_DIR, exist_ok=True)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(TEMP_UNZIP_DIR)
        # list dir under TEMP_UNZIP_DIR and remove __MACOSX dir and contents
        if os.path.isdir(os.path.join(TEMP_UNZIP_DIR, '__MACOSX')):
            shutil.rmtree(os.path.join(TEMP_UNZIP_DIR, '__MACOSX'))  # remove __MACOSX dir and contents
        # list all files under TEMP_UNZIP_DIR with relative path
        files = []
        # walk through the TEMP_UNZIP_DIR and get all files and skip .DataStore
        for root, _, filenames in os.walk(TEMP_UNZIP_DIR):
            for file_name in filenames:
                if file_name.startswith('.DS_Store'):
                    continue
                extracted_file_path = os.path.relpath(os.path.join(root, file_name), TEMP_UNZIP_DIR)
                in_archive_manifest = any(row.get(FILE_PATH) == extracted_file_path for row in archived_files_info)
                if in_archive_manifest:
                    files.append(extracted_file_path)
                else:
                    log.error(f"File {extracted_file_path} found in zip file {file_path} is not included in archive manifest!")
        if len(files) != len(archived_files_info):
            # find files in archived_files_info but not in files
            missing_files = [row.get(FILE_PATH) for row in archived_files_info if row.get(FILE_PATH) not in files]
            invalid_reason = f"The zip file  {file_path} is missing the following files: {', '.join(missing_files)}"
            log.error(invalid_reason)
            return False
        rtnVal = True
        for file_name in files:
            file_info = next((row for row in archived_files_info if row.get(FILE_PATH) == file_name), None)
            file_path = os.path.join(TEMP_UNZIP_DIR, file_name)
            # file size
            file_size = os.path.getsize(file_path)
            if file_size != int(file_info[FILE_SIZE_DEFAULT]):
                invalid_reason = f"Real file size {file_size} of file {file_name} does not match with that in archive manifest {file_info[FILE_SIZE_DEFAULT]}!"
                log.error(invalid_reason)
                rtnVal = False
                continue
            # md5
            md5sum = get_file_md5(file_path, md5_cache, file_size, log)
            if md5sum != file_info[MD5_DEFAULT]:
                invalid_reason = f"Real file md5 {md5sum} of file {file_name} does not match with that in archive manifest {file_info[MD5_DEFAULT]}!"
                log.error(invalid_reason)
                rtnVal = False
        return rtnVal
    except Exception as e:
        log.error(f"Failed to validate zip file contents: {e}")
        return False
    finally:
        # remove contents of the temporary directory
        if os.path.isdir(TEMP_UNZIP_DIR):
            shutil.rmtree(TEMP_UNZIP_DIR)

def get_file_md5(file_path, md5_cache, file_size, log):
    """
    retrieve md5 if existing cached value, otherwise calculate md5 for the file and save to md5 cache
    """
    file_modified_at = os.path.getmtime(file_path)
    # check if md5 is in cache by file name and file size
    cached_md5 = [row[MD5_DEFAULT] for row in md5_cache if row[FILE_PATH] == file_path and row[FILE_SIZE_DEFAULT] == str(file_size) and 
                    row[MODIFIED_AT] == str(file_modified_at)] if md5_cache else None
    if not cached_md5 or len(cached_md5) == 0:
         #calculate file md5
        md5sum = calculate_file_md5(file_path, file_size, log)
        if isinstance(md5_cache, list): 
            md5_cache.append({FILE_PATH: file_path, FILE_SIZE_DEFAULT: file_size, MD5_DEFAULT: md5sum, MODIFIED_AT: file_modified_at})
    else:
        md5sum = cached_md5[0]

    return md5sum

def check_payload_size(file_info_list, configs, log):
    """
    Check if the payload size of files_info is within the limit.
    If not, raise an exception.
    """
    file_array = [item.get(FILE_NAME_DEFAULT) for item in file_info_list]
    submissionId = configs.get(SUBMISSION_ID)
    type = configs.get(UPLOAD_TYPE)
    payload= f"""
        mutation {{
            createBatch (
                submissionID: \"{submissionId}\", 
                type: \"{type}\", 
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
    body_size = len(payload.encode("utf-8"))
    if body_size > MAX_CREATE_BATCH_PAYLOAD_SIZE:
        log.error(f"create batch body size is too large: {body_size} with {len(file_array)} files, please reduce the number of files for one batch.")
        return False
    return True
