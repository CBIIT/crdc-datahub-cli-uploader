# crdc-datahub-upload-cli

CRDC datahub upload CLI is a command line interface application for end users to upload cancer research data files and metadata to Datahub.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3 and so on. All required python modules is listed in the file, requirements.txt, and .gitsubmodules.

The application is consist of multiple python modules/classes to support multiple functions listed below:

1) Validate local and remote data files and metadata files by verifying md5 and size of files.
2) Create uploading batch via crdc-datahub backend API.
3) Create AWS STS temp credential for uploading files to s3 bucket.
4) Upload both data files and metadata files to S3 bucket.
5) Update uploading batch via crdc-datahub backend API.
6) Create uploading report.
7) Log info, error and exceptions.

Major implemented modules/classes in src dir:

1) uploader.py
    This is the entry point of the command line interface.  It controls the workflow and dispatches different requests to designated modules/classes.

2) upload_config.py
    This class manages request arguments and configurations.  It receives user's arguments, validate these arguments and store them in a dictionary.

3) file_validator.py
    This class validates 1) data files by checking file size and md5; 2) metadata files by checking if file existing in the data folder defined by user.
    During the validation, it also constructs a dictionary to record data file path, data file size, validate result, validation message for data files.  For metadata files, it constructs a dictionary only with file path and file size.

4) common/graphql_client.py
    This class hosts three clients for three graphql APIs, createTempCredential, createBatch, updateBatch.

5) common/s3util.py
    This utility class is for access designated s3 bucket with temp AWS STS credentials, and upload files to the bucket designated by uploading batch.

6) file_uploader.py
    This class manages a job queue for uploading valid data files (big size) and metadata files (small size) from local to S3 bucket via copier.py.

7) copier.py
    This class processes each job passed by file-uploader.py, then either upload or put into s3 bucket based on upload type, data file|metadata, and size via S3util.py.

8) common/utils.py
    This module provides utility functions such as dumping dictionary to tsv file, extracting exception code and messages.

Usage of the CLI tool:

1) Get helps command
    $ python src/uploader.py -h
    ##Executing results:
    Command line arguments / configuration
    -a --api-url, API endpoint URL, required
    -k --token, API token string, required
    -u --submission, submission ID, required
    -t --type, valid value in [“data file”, “metadata”], required
    -d --data, folder that contains either data files (type = “data file”) or metadata (TSV/TXT) files (type = “metadata”), required
    -c --config, configuration file path, can potentially contain all above parameters, preferred
    -r --retries, file uploading retries, integer, optional, default value is 3
    Following arguments are needed to read important data from manifest, conditional required when type = “data file”

    -m --manifest, path to manifest file, conditional required when type = “data file”
    -n --name-field
    -s --size-field
    -m --md5-field
    -i --id-field
    -o --omit-DCF-prefix

    CLI configuration module will validate and combine parameters from CLI and/or config file
    If config_file is given, then everything else is potentially optional (if it’s included in config file)
    Some arguments are only needed for type = “data file” or type = “metadata”

2) Upload data files command
    $ python src/uploader.py -c configs/test-file-upload.yml

3) Upload metadata command
    $ python src/uploader.py -c configs/test-metadata-upload.yml

