import csv, os
import pandas as pd
from common.constants import FILE_ID_DEFAULT, FILE_NAME_FIELD, BATCH_BUCKET, S3_BUCKET, FILE_PREFIX, BATCH_ID, DCF_PREFIX, BATCH_CREATED,\
    FILE_ID_FIELD, UPLOAD_TYPE, FILE_NAME_DEFAULT, FILE_PATH, FILE_SIZE_DEFAULT, BATCH_STATUS, PRE_MANIFEST, OMIT_DCF_PREFIX,\
    TEMP_DOWNLOAD_DIR, FROM_S3, SUBFOLDER_FILE_NAME
from common.graphql_client import APIInvoker
from copier import Copier
from common.s3util import S3Bucket

SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
NODE_TYPE_NAME = 'type'
def process_manifest_file(log, configs, has_file_id, file_infos, manifest_rows, manifest_s3_url):
    """
    function: process_manifest_file
    params:
     configs: the config object of uploader
     file_path: the path of the pre-manifest file
     has_file_id: whether the pre-manifest file has file id column or not
     file_infos: the file info array of the pre-manifest file
     manifest_rows: the rows of the pre-manifest file
    return:
     True or False

     steps:
     1) add file id to the pre-manifest file if no file id column
     2) create a batch for upload the final manifest file
     3) upload the final manifest file to S3
     4) update the batch with file info.
    """
    if not file_infos or len(file_infos) == 0:
        log.info(f"Failed to add file id to the pre-manifest, {file_path}.")
        return False
    # check if file in file_infos has SUBFOLDER_FILE_NAME
    hasSubFolder = any(d for d in file_infos if d.get(SUBFOLDER_FILE_NAME))
    
    needFinalManifest = hasSubFolder or (not has_file_id)
    file_path = configs.get(PRE_MANIFEST)
    final_manifest_path = (str.replace(file_path, ".tsv", "-final.tsv") if ".tsv" in file_path else str.replace(file_path, ".txt", "-final.tsv")) if needFinalManifest else file_path
    file_id_name = configs[FILE_ID_FIELD]
    file_name_name = configs[FILE_NAME_FIELD]
    result = None
    newBatch = None
    final_file_path_list = []
    file_array = []
    try:
        if needFinalManifest:
            result = add_file_id(file_id_name, file_name_name, final_manifest_path , file_infos, manifest_rows, configs.get(OMIT_DCF_PREFIX))
            if not result:
                log.info(f"Failed to add file id to the pre-manifest, {final_manifest_path }.")
                return False
        
        configs[UPLOAD_TYPE] = "metadata"
        final_file_path_list = [final_manifest_path]
        # insert file id into children tsv files.
        insert_file_id_2_children(log, configs, manifest_rows, final_file_path_list, manifest_s3_url)
        file_array = [os.path.basename(file_path) for file_path in final_file_path_list]
        # create a batch for upload the final manifest file
        apiInvoker = APIInvoker(configs)
        if apiInvoker.create_batch(file_array):
            newBatch = apiInvoker.new_batch
            if not newBatch.get(BATCH_BUCKET) or not newBatch[FILE_PREFIX] or not newBatch.get(BATCH_ID):
                log.info("Failed to upload files: can't create new batch! Please check log file in tmp folder for details.")
                return False
            configs[S3_BUCKET] = newBatch.get(BATCH_BUCKET)
            configs[FILE_PREFIX] = newBatch[FILE_PREFIX]
            configs[BATCH_ID] = newBatch.get(BATCH_ID)
            log.info(f"New batch is created: {newBatch.get(BATCH_ID)} at {newBatch[BATCH_CREATED]}")
            uploader = Copier(configs[S3_BUCKET], configs[FILE_PREFIX] , configs)
            for file_path in final_file_path_list:
                result = uploader.copy_file({FILE_NAME_DEFAULT: os.path.basename(file_path), FILE_PATH: file_path, FILE_SIZE_DEFAULT: os.path.getsize(file_path)}, True, False)  
            log.info(f'The manifest and final metadata files, {file_array}, have been uploaded to destination successfully.')
    except Exception as e:
        log.info(f"Failed to add file id to the pre-manifest, {file_path}. Error: {e}") 
    finally:
        if not newBatch or not result:
            log.info(f"Failed process the manifest and final metadata files, {file_array}.")
            return False
        else:
            # update batch
            status = result.get(BATCH_STATUS, False)
            errors = [f"Failed to upload manifest and final metadata files,{file_array}"] if not status else []
            # manifest_file_info = {"fileName": final_manifest_name, "succeeded": status, "errors": errors, "skipped": False}
            final_file_info_list  = [ {"fileName": os.path.basename(file_path), "succeeded": status, "errors": errors, "skipped": False} for file_path in final_file_path_list]
            if not apiInvoker.update_batch(newBatch[BATCH_ID], final_file_info_list):
                log.info(f"Failed to update batch, {newBatch[BATCH_ID]}!")
                return False
            log.info(f"Successfully process the manifest and added file id into children tsv files, {file_array}.")
        return True

# This method will create a new manifest file with the file id column added to the pre-manifest and internal_file_name.
def add_file_id(file_id_name, file_name_name, final_manifest_path, file_infos, manifest_rows, omit_prefix):
    output = []
    for row in manifest_rows:
        file = [file for file in file_infos if file[FILE_NAME_DEFAULT] == row[file_name_name]][0]
        file[FILE_ID_DEFAULT] = file[FILE_ID_DEFAULT] if omit_prefix == False else file[FILE_ID_DEFAULT].replace(DCF_PREFIX, "")
        row[file_name_name] = os.path.basename(file[FILE_NAME_DEFAULT])
        row[SUBFOLDER_FILE_NAME] = file[SUBFOLDER_FILE_NAME] if SUBFOLDER_FILE_NAME in file else ""
        row[file_id_name] = file[FILE_ID_DEFAULT]
        output.append(row.values())
    manifest_columns = manifest_rows[0].keys()
    with open(final_manifest_path, 'w', newline='') as f: 
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(manifest_columns)
        writer.writerows(output)
    return True

# insert file node ID into relationship data fiels in children's metadata file.
def insert_file_id_2_children(log, configs, manifest_rows, final_file_path_list, manifest_s3_url):
     # check if any tsv files in the dir of manifest file
    manifest_file = configs.get(PRE_MANIFEST)
    is_s3 = configs.get(FROM_S3, False)
    dir = os.path.dirname(manifest_file) if not is_s3 else TEMP_DOWNLOAD_DIR
    s3_bucket = None
    try:
        if is_s3:
            s3_bucket = S3Bucket()
            # download tsv or txt files from s3 to TEMP_DOWNLOAD_DIR
            download_meatadata_in_s3(manifest_s3_url, s3_bucket)
            
        tsv_files = [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) 
                    and (f.endswith('.tsv') or f.endswith('.txt')) and f not in manifest_file and not "-final." in f]
        file_type = manifest_rows[0].get(NODE_TYPE_NAME) 
        children_files = []
        if file_type:
            file_id_to_check = f"{file_type}.{configs.get(FILE_ID_FIELD)}"
            if len(tsv_files) > 0:
                for file in tsv_files:
                    # check if tsv file's header contains 
                    with open(file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        header = next(reader)  # get the first row
                        if file_id_to_check in header:
                            children_files.append(file)
                        else:
                            # remove the file if in temp dir
                            if is_s3:
                                os.remove(file)
            if len(children_files) > 0:
                for file in children_files:
                    inserted = False
                    # read tsv file to dataframe
                    df = pd.read_csv(file, sep=SEPARATOR_CHAR, header=0, dtype='str', encoding=UTF8_ENCODE,keep_default_na=False,na_values=[''])
                    if file_id_to_check in df.columns:
                        for index, row in df.iterrows():
                            fileName = row[file_id_to_check]
                            if fileName:
                                file_info = next((file for file in manifest_rows if file[configs[FILE_NAME_FIELD]] == fileName), None)
                                if file_info:
                                    file_id = file_info[configs[FILE_ID_FIELD]]
                                    df.at[index, file_id_to_check] = file_id
                                    inserted = True
                        if inserted:
                            file_ext = '.tsv' if file.endswith('.tsv') else '.txt'
                            final_file_path = file.replace(file_ext, f'-final{file_ext}')
                            df.to_csv(final_file_path, sep ='\t', index=False)
                            final_file_path_list.append(final_file_path)
                        else:
                            # remove the file if in temp dir
                            if is_s3:
                                os.remove(file)
    except Exception as e:
        log.exception(f"Failed to insert file id into children tsv files. Error: {e}")
    finally:
        if s3_bucket:
            s3_bucket = None

"""
download tsv or txt files from s3 to TEMP_DOWNLOAD_DIR
"""
def download_meatadata_in_s3(manifest_file_path, s3_bucket):
    #  s3://crdcdh-test-submission/9f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/file.txt
    bucket, prefix, manifest_file = get_s3_bucket_and_prefix(manifest_file_path)
    # download all files with ext "tsv" or "txt" in the folder of prefix from s3
    s3_bucket.set_s3_client(bucket, None)
    metadata_files = s3_bucket.get_contents_in_current_folder(prefix)
    for file in metadata_files:
        if not manifest_file in file:
            file_name = file.split("/")[-1]
            s3_bucket.download_object(file, os.path.join(TEMP_DOWNLOAD_DIR,file_name))
"""
upload metadata file into s3
"""
def upload_metadata_to_s3(manifest_file_path, file_path, s3_bucket):
    bucket, prefix, _ = get_s3_bucket_and_prefix(manifest_file_path)
    s3_bucket.set_s3_client(bucket, None)
    file_name = file_path.split("/")[-1]
    key = os.path.join(prefix, file_name)
    s3_bucket.put_file(key, file_path)

"""
get bucket and prefix from manifest file path
"""
def get_s3_bucket_and_prefix(manifest_file_path):
    # manifest_file_path eg. s3://crdcdh-test-submission/9f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/file.txt
    manifest_file_path = manifest_file_path.replace("s3://", "")
    temp = manifest_file_path.split("/")
    bucket = temp[0]
    prefix = "/".join(temp[1:-1]) + "/"
    manifest_file = temp[-1]
    return bucket, prefix, manifest_file


    

     
   
                