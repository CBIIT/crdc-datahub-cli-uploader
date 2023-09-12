# crdc-datahub-upload-cli
CRDC datahub upload cli is a command line interface application for end users to upload cancer research files and metadata to Datahub.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3, neo4j and so on. All required python modules is listed in the file, requirements.txt, and .gitsubmodules.

The application is consist of multiple python modules/classes to support mutiple functions listed below:

1) Validate local and remote files by verifying md5 and size of files.
2) Validate metadata by checking data models.
3) upload both files and metadata files to S3 bucket.


