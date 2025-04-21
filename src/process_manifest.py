import csv, os, io
import pandas as pd
import numpy as np
from common.constants import FILE_ID_DEFAULT, FILE_NAME_FIELD, BATCH_BUCKET, S3_BUCKET, FILE_PREFIX, BATCH_ID, DCF_PREFIX, BATCH_CREATED,\
    FILE_ID_FIELD, UPLOAD_TYPE, FILE_NAME_DEFAULT, FILE_PATH, FILE_SIZE_DEFAULT, BATCH_STATUS, PRE_MANIFEST, OMIT_DCF_PREFIX,\
    TEMP_DOWNLOAD_DIR
from common.graphql_client import APIInvoker
from copier import Copier
from common.s3util import S3Bucket

SEPARATOR_CHAR = '\t'
UTF8_ENCODE ='utf8'
def process_manifest_file(log, configs, has_file_id, file_infos, manifest_rows, manifest_columns):
    """
    function: process_manifest_file
    params:
     configs: the config object of uploader
     file_path: the path of the pre-manifest file
     has_file_id: whether the pre-manifest file has file id column or not
     file_infos: the file info array of the pre-manifest file
     manifest_rows: the rows of the pre-manifest file
     manifest_columns: the columns of the pre-manifest file
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
    file_path = configs.get(PRE_MANIFEST)
    final_manifest_path = (str.replace(file_path, ".tsv", "-final.tsv") if ".tsv" in file_path else str.replace(file_path, ".txt", "-final.tsv")) if not has_file_id else file_path
    file_id_name = configs[FILE_ID_FIELD]
    file_name_name = configs[FILE_NAME_FIELD]
    manifest_columns.append(file_id_name)
    result = None
    newBatch = None
    manifest_file_info = None
    try:
        if not has_file_id:
            result = add_file_id(file_id_name, file_name_name, final_manifest_path , file_infos, manifest_rows, manifest_columns, configs.get(OMIT_DCF_PREFIX))
            if not result:
                log.info(f"Failed to add file id to the pre-manifest, {final_manifest_path }.")
                return False
        # create a batch for upload the final manifest file
        manifest_file_size = os.path.getsize(final_manifest_path)
        manifest_file_info = {"fileName": final_manifest_path, "size": manifest_file_size} 
        configs[UPLOAD_TYPE] = "metadata"
        # add file_id into child tsv files
        apiInvoker = APIInvoker(configs)
        final_manifest_name = os.path.basename(final_manifest_path)
        #file_array = [{"fileName": final_manifest_name, "size": manifest_file_size}]
        file_array = [final_manifest_name]
        newBatch = None
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
            result = uploader.copy_file({FILE_NAME_DEFAULT: final_manifest_name, FILE_PATH: final_manifest_path, FILE_SIZE_DEFAULT: manifest_file_size}, True, False)  
            log.info(f'The manifest, "{final_manifest_name}" has been uploaded to destination successfully.')
    except Exception as e:
        log.info(f"Failed to add file id to the pre-manifest, {file_path}. Error: {e}") 
    finally:
        if not newBatch or not result:
            log.info(f"Failed process the manifest, {final_manifest_path}.")
            return False
        else:
            # update batch
            status = result.get(BATCH_STATUS, False)
            errors = [f"Failed to upload manifest file,{final_manifest_name}"] if not status else []
            manifest_file_info = {"fileName": final_manifest_name, "succeeded": status, "errors": errors, "skipped": False}
            if not apiInvoker.update_batch(newBatch[BATCH_ID], [manifest_file_info]):
                log.info(f"Failed to update batch, {newBatch[BATCH_ID]}!")
                return False
            log.info(f"Successfully process the manifest, {final_manifest_path}.")
        return True

# This method will create a new manifest file with the file id column added to the pre-manifest.
def add_file_id(file_id_name, file_name_name, final_manifest_path, file_infos, manifest_rows, manifest_columns, omit_prefix):
    output = []
    for row in manifest_rows:
        file = [file for file in file_infos if file["fileName"] == row[file_name_name]][0]
        file[FILE_ID_DEFAULT] = file[FILE_ID_DEFAULT] if omit_prefix == False else file[FILE_ID_DEFAULT].replace(DCF_PREFIX, "")
        row[file_id_name] = file[FILE_ID_DEFAULT]
        output.append(row.values())
    with open(final_manifest_path, 'w', newline='') as f: 
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(manifest_columns)
        writer.writerows(output)
    return True

def insert_file_id_2_children(configs, manifest_rows, is_s3, manifest_s3_url):
     # check if any tsv files in the dir of manifest file
    manifest_file = configs.get(PRE_MANIFEST) if not is_s3 else manifest_s3_url
    dir = os.path.dirname(manifest_file) if not is_s3 else TEMP_DOWNLOAD_DIR
    s3_bucket = None
    try:
        if is_s3:
            s3_bucket = S3Bucket()
            # download tsv or txt files from s3 to TEMP_DOWNLOAD_DIR
            download_meatadata_in_s3(manifest_file, s3_bucket)
            
        tsv_files = [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) 
                    and (f.endswith('.tsv') or f.endswith('.txt'))]
        file_type = manifest_rows[0].get(UPLOAD_TYPE)
        if file_type:
            file_id_to_check = f"{file_type}.{configs.get(FILE_ID_FIELD)}"
            if len(tsv_files) > 0:
                children_files = []
                for file in tsv_files:
                    # check if tsv file's header contains 
                    with open(file) as f:
                        reader = csv.DictReader(f, delimiter='\t')
                        header = next(reader)  # get the first row
                        if file_id_to_check in header:
                            children_files.append(file)
            if len(children_files) > 0:
                for file in children_files:
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

                        file_ext = '.tsv' if file.endswith('.tsv') else '.txt'
                        final_file_path = file.replace(file_ext, f'-final{file_ext}')

                        df.to_csv(final_file_path, sep ='\t', index=False)
                        if is_s3:
                            # upload final metadata file into s3
                            upload_metadata_to_s3(manifest_file, final_file_path, s3_bucket)
    finally:
        if s3_bucket:
            s3_bucket = None
                
def download_meatadata_in_s3(manifest_file_path, s3_bucket):
    #  s3://crdcdh-test-submission/9f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/file.txt
    bucket, prefix, manifest_file = get_s3_bucket_and_prefix(manifest_file_path)
    # download all files with ext "tsv" or "txt" in the folder of prefix from s3
    s3_bucket.set_s3_client(bucket, None)
    metadata_files = s3_bucket.get_contents(prefix)
    for file in metadata_files:
        if not manifest_file in file:
            file_name = file.split("/")[-1]
            s3_bucket.download_object(file, os.path.join(TEMP_DOWNLOAD_DIR,file_name))

def upload_metadata_to_s3(manifest_file_path, file_path, s3_bucket):
    bucket, prefix, _ = get_s3_bucket_and_prefix(manifest_file_path)
    # upload all files with ext "tsv" or "txt" in the folder of prefix from s3
    s3_bucket.set_s3_client(bucket, None)
    file_name = file_path.split("/")[-1]
    key = os.path.join(prefix, file_name)
    s3_bucket.put_file(key, file_path)

def get_s3_bucket_and_prefix(manifest_file_path):
     #  s3://crdcdh-test-submission/9f42b5f1-5ea4-4923-a9bb-f496c63362ce/file/file.txt
    manifest_file_path = manifest_file_path.replace("s3://", "")
    temp = manifest_file_path.split("/")
    bucket = temp[0]
    prefix = "/".join(temp[1:-1])
    manifest_file = temp[-1]
    return bucket, prefix, manifest_file


    

     
   
                