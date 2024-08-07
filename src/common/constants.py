#define constants, enums, etc.
#config 
UPLOAD_TYPE = "type"
TYPE_FILE ="data file"
TYPE_MATE_DATA= "metadata"
UPLOAD_TYPES = [TYPE_FILE, TYPE_MATE_DATA]
INTENTION = "intention"
INTENTION_NEW = "Add"
INTENTION_UPDATE = "Add/Change"
INTENTION_DELETE = "Remove"
INTENTIONS = [INTENTION_NEW, INTENTION_UPDATE, INTENTION_DELETE]
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
OVERWRITE = "overwrite"
DRY_RUN = "dryrun"

#file validation 
FILE_INVALID_REASON = "invalid_reason"
SKIPPED = "skipped"

#upload
UPLOAD_STATUS ="upload_status"

#Bacth
BATCH_BUCKET = "bucketName"
FILE_PREFIX = "filePrefix" #bucket folders
BATCH = "batch"
BATCH_ID = "_id"
SUCCEEDED = "succeeded"
ERRORS = "errors"
BATCH_CREATED = "createdAt"
BATCH_UPDATED = "updatedAt"
BATCH_STATUS= "status"

#credential
TEMP_CREDENTIAL = "credentials"
ACCESS_KEY_ID = "accessKeyId"
SECRET_KEY = "secretAccessKey"
SESSION_TOKEN = "sessionToken"


