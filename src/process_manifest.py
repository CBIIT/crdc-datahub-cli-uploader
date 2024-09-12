import csv, os, io
from common.constants import FILE_ID_DEFAULT, FILE_NAME_FIELD, BATCH_BUCKET, S3_BUCKET, FILE_PREFIX, BATCH_ID, DCF_PREFIX, BATCH_CREATED,\
    FILE_ID_FIELD, UPLOAD_TYPE, FILE_NAME_DEFAULT, FILE_PATH, FILE_SIZE_DEFAULT, BATCH_STATUS, PRE_MANIFEST, OMIT_DCF_PREFIX
from common.graphql_client import APIInvoker
from copier import Copier


def process_manifest_file(configs, has_file_id, file_infos, manifest_rows, manifest_columns):
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
        print(f"Failed to add file id to the pre-manifest, {file_path}.")
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
                print(f"Failed to add file id to the pre-manifest, {final_manifest_path }.")
                return False
        # create a batch for upload the final manifest file
        manifest_file_size = os.path.getsize(final_manifest_path)
        manifest_file_info = {"fileName": final_manifest_path, "size": manifest_file_size} 
        configs[UPLOAD_TYPE] = "metadata"
        apiInvoker = APIInvoker(configs)
        final_manifest_name = os.path.basename(final_manifest_path)
        file_array = [{"fileName": final_manifest_name, "size": manifest_file_size}]
        newBatch = None
        if apiInvoker.create_batch(file_array):
            
            newBatch = apiInvoker.new_batch
            if not newBatch.get(BATCH_BUCKET) or not newBatch[FILE_PREFIX] or not newBatch.get(BATCH_ID):
                print("Failed to upload files: can't create new batch! Please check log file in tmp folder for details.")
                return False
            configs[S3_BUCKET] = newBatch.get(BATCH_BUCKET)
            configs[FILE_PREFIX] = newBatch[FILE_PREFIX]
            configs[BATCH_ID] = newBatch.get(BATCH_ID)
            print(f"New batch is created: {newBatch.get(BATCH_ID)} at {newBatch[BATCH_CREATED]}")
            uploader = Copier(configs[S3_BUCKET], configs[FILE_PREFIX] , configs)
            result = uploader.copy_file({FILE_NAME_DEFAULT: final_manifest_name, FILE_PATH: final_manifest_path, FILE_SIZE_DEFAULT: manifest_file_size}, True, False)   
    except Exception as e:
        print(f"Failed to add file id to the pre-manifest, {file_path}. Error: {e}") 
    finally:
        if not newBatch or not result:
            print(f"Failed process the manifest, {final_manifest_path}.")
            return False
        else:
            # update batch
            status = result.get(BATCH_STATUS, False)
            errors = [f"Failed to upload manifest file,{final_manifest_name}"] if not status else []
            manifest_file_info = {"fileName": final_manifest_name, "succeeded": status, "errors": errors, "skipped": False}
            if not apiInvoker.update_batch(newBatch[BATCH_ID], [manifest_file_info]):
                print(f"Failed to update batch, {newBatch[BATCH_ID]}!")
                return False
            print(f"Successfully process the manifest, {final_manifest_path}.")
        return True

# This method will create a new manifest file with the file id column added to the pre-manifest.
def add_file_id(file_id_name, file_name_name, final_manifest_path, file_infos, manifest_rows, manifest_columns, omit_prefix):
    output = []
    for file in file_infos:
        row = [row for row in manifest_rows if row[file_name_name] == file["fileName"]][0]
        file[FILE_ID_DEFAULT] = file[FILE_ID_DEFAULT] if omit_prefix == False else file[FILE_ID_DEFAULT].replace(DCF_PREFIX, "")
        row[file_id_name] = file[FILE_ID_DEFAULT]
        output.append(row.values())
    with open(final_manifest_path, 'w', newline='') as f: 
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(manifest_columns)
        writer.writerows(output)
    return True