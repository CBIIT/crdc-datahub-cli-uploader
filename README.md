# crdc-datahub-upload-cli
CRDC datahub upload cli is a command line interface application for end users to upload cancer research files and metadata to Datahub.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3, neo4j and so on. All required python modules is listed in the file, requirements.txt, and .gitsubmodules.

The application is consist of multiple python modules/classes to support mutiple functions listed below:

1) Validate local and remote files by verifying md5 and size of files.
2) Validate metadata by checking data models.
3) upload both files and metadata files to S3 bucket.

Major implemented modules/classred in src dir:
1) uploader.py
    This is the entry point of the command line interface.  It controls the workflow and dispatchs different requests to designated modules/classes.

2) upload_config.py
    This class manages request arguments and configurations.  It receives user's arguments, validate these arguments and hold them in a dictionary.

3) file_validator.py
    This class validates 1) study files by checking file size and md5; 2) metadata files by checking if file existing in the data folder defined by user.
    During the validation, it also constracts a dictionary to record file path, file size, validate result, validateion message for study files.  For metadata files, it constructs a dictionary only with file path and file size.

4) common/graphql_client.py
    This module hosts three clients for three graphql APIs, createTempCredential, createBatch, updateBatch.

5) common/s3util.py
    This utility class is for access designated s3 bucket based on temp credetails and upload files to the bucket.

6) file_uploader.py
    This class manages a job queue for uploading valid study files (big files) and metadata files (small files) from local to S3 bucket via copier.py.

7) copier.py
    This class process each job passed by file-uploader.py and eith upload or put into s3 buket based on upload type, file|metadata, and size via S3util.py.




