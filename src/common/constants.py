#define constants, enums, etc.
#config 
UPLOAD_TYPE = "type"
UPLOAD_TYPES = ["file", "metadata"]
INTENTION = "intention"
INTENTIONS =  ["New", "Update", "Delete"]
FILE_NAME_FIELD = "name-field"
FILE_NAME_DEFAULT = "file_name"  #match data model file name 
FILE_PATH = "file_path"
FILE_SIZE_FIELD = "size-field"
FILE_SIZE_DEFAULT = "file_size"  #match data model file size name
FILE_MD5_FIELD = "md5-field"
MD5_DEFAULT = "md5sum" #match data model md5 name
TOKEN = "token"
API_URL = "api-url"
SUBMISSION_ID = "submission"
PRE_MANIFEST = "manifest"
FILE_DIR = "data"
S3_BUCKET = "bucket"
RETRIES = "retries"

#file validation 
FILE_INVALID_REASON = "invalid_reason"

#upload
UPLOAD_STATUS ="upload_status"

#Bacth
BATCH_BUCKET = "bucketName"
FILE_PREFIX = "filePrefix" #bucket folders
BATCH = "batch"
BATCH_ID = "_id"
SUCCEEDED = "succeeded"
ERRORS = "errors"


#credential
TEMP_CREDENTIAL = "credentials"
ACCESS_KEY_ID = "accessKeyId"
SECRET_KEY = "secretAccessKey"
SESSION_TOKEN = "sessionToken"


