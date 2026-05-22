#define constants, enums, etc.
#config 
UPLOAD_TYPE = "type"
TYPE_FILE ="data file"
TYPE_MATE_DATA= "metadata"
UPLOAD_TYPES = [TYPE_FILE, TYPE_MATE_DATA]
FILE_NAME_FIELD = "name-field"
FILE_NAME_DEFAULT = "file_name"  #match data model file name 
FILE_PATH = "file_path"
FILE_SIZE_FIELD = "size-field"
FILE_SIZE_DEFAULT = "file_size"  #match data model file size name
FILE_MD5_FIELD = "md5-field"
FILE_ID_FIELD = "id-field"
FILE_ID_DEFAULT = "fileID"
OMIT_DCF_PREFIX = "omit-DCF-prefix"
DCF_PREFIX = "dg.4DFC/"
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
BYPASS_ARCHIVE_VALIDATION = "bypass_archive_validation"

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

S3_START= "s3://"
FROM_S3 = "from_s3"
TEMP_DOWNLOAD_DIR = "tmp/download"

CLI_VERSION = "4.3"
MD5_CACHE_DIR = "tmp/md5"
MD5_CACHE_FILE = "md5_cache.csv"
MODIFIED_AT = "modifiedAt"
HEARTBEAT_INTERVAL_CONFIG = "heartbeat_interval"
CURRENT_UPLOADER_VERSION_CONFIG = "current_uploader_version"
SUBFOLDER_FILE_NAME = "internal_file_name"
SEPARATOR_CHAR = '\t'

TEMP_UNZIP_DIR = "tmp/unzip"
ARCHIVE_MANIFEST = "archive_manifest"
ARCHIVE_NAME = "archive_name"
MAX_CREATE_BATCH_PAYLOAD_SIZE = 1024 * 1024 * 5  # 5MB. The create batch payload size is half to 75% of updated batch size.
MAX_UPDATE_BATCH_PAYLOAD_SIZE = 1024 * 1024 * 10  # 10MB
TEMP_TOKEN_DURATION = "temp_token_duration"
TEMP_TOKEN_EXPIRATION = "expiration"
MAX_DELETE_RETRY = 2

