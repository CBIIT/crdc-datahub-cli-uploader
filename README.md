# CRDC Data Hub CLI Uploader 

## Introduction

CRDC Data Hub CLI Uploader (will be referred to as “CLI tool” or “CLI” in the rest of this document) is a command line tool designed to help data submitters to upload files or metadata to Data Hub submission S3 bucket as a part of a submission created in the Data Hub.

The CLI tool was designed to run on data submitter’s computer. It communicates with Data Hub backend services to verify user’s identity and permission, uploads files and metadata to Data Hub submission S3 bucket as well as create “batch” records in Data Hub’s database which will be viewable on Data Hub’s web portal. 

## Prerequisites

The CLI tool was written in Python3 and requires following environment/software to be installed.

- A Mac or Windows computer
- Python3
- Git
- An active Data Hub user with Submitter or Organization Owner roles

## Download and Installation

- Open a terminal window (on Mac) or command line window (on Windows)
- Switch to the desired folder for the CLI tool to be installed (‘$’ is not part of the command)

  `$ cd desired_installation_folder`
 
- Type of paste in following commands, press enter/return after each command (‘$’ is not part of the command)
- Download CLI tool
  - git clone --recurse-submodules https://github.com/CBIIT/crdc-datahub-cli-uploader.git
  -	Switch to CLI folder. All commands in the reset of this document needs to be executed inside the CLI folder.
  
    `$ cd crdc-datahub-cli-uploader`
  - Install dependencies. Depends on how Python3 was installed, on some systems you need to use “pip” instead of “pip3” in following command.
  
    `$ pip3 install -r requirements.txt`

## Gather Information from Data Hub

### Download Data Hub API token
	
- Log into Data Hub web portal
- Click your name near the top right corner
- Click “API TOKEN” in the menu
- Click “Create Token” button
- Click the “copy” button next to the text box
- Pasted the token into a text file for future steps

### Copy submission ID

- Log into Data Hub web portal
- Click “Data Submission”
- (Optional) Click “Create a Data Submission” if you want to create a new submission
- In the submission table, click the submission name of the submission you’d like to upload files or metadata to
- Click the “copy” button next to submission ID
- Pasted the submission ID into a text file for future steps

## Upload files

### Prepare files

Put all files to be uploaded in the same folder.

### Prepare manifest

A manifest is a special metadata (TSV) file that contains information about files to be uploaded. CLI tool will use the information in a manifest to find, validate and upload files to Data Hub. There are 3 columns that are important to CLI tool:
- Column contains file names
- Column contains file sizes
- Column contains file MD5 checksums

Different Data Commons may have different column names, but they all contain the same information.

You can put a manifest in the same folder with the files, or you can put it in a separate folder.

### Prepare configuration file
- Make a copy of the example config file: “crdc-datahub-cli-uploader/configs/uploader-file-config.example.yml”, give it an appropriate name, in this document we name it “file-upload.yml”
- Open the new config file with a text editor, preferably a code editor like Sublime Text, Notepad++, VSCode, Vim, Emacs etc. Please DO NOT use a word processor like Word or Pages to open the config file.
- Configurations are in “key: value” format. There must be a space between colon and the value.
- api-url: keep it unchanged, unless you are using an environment other than Data Hub production environment
- token: paste in the API token saved in previous steps
- submission: paste in the submission ID saved in previous steps 
- type: must be set to “file”
- data: path to the folder that contains the files to be uploaded
- manifest: path to the manifest file
- name-field: column name in the manifest file that contains file names
- size-field: column name in the manifest file that contains file sizes
- md5-field: column name in the manifest file that contains file MD5 checksums
- retries: number of retries the CLI tool will perform after a failed upload 
- overwrite: if set to “true”, CLI will upload a file to overwrite the file with same name that already exists in the Data Hub submission S3 bucket. If set to “false”, CLI will not upload a file if a file with the same name exists in the Data Hub submission S3 bucket.
- dryrun: if set to “true”, CLI will not upload any files to the Data Hub submission S3 bucket. If set to “false”, CLI will upload files to the Data Hub submission S3 bucket.

### Execute upload command

Depends on how Python3 was installed, on some systems you need to use “python” instead of “python3” in following command.

`$ python3 src/uploader.py --config configs/file-upload.yml`

## Upload metadata

### Prepare metadata

Put all metadata (TSV) files to be uploaded in the same folder.

### Prepare configuration file
- Make a copy of the example config file: “crdc-datahub-cli-uploader/configs/ uploader-metadata-config.example.yml”, give it an appropriate name, in this document we name it “metadata-upload.yml”
- Open the new config file with a text editor, preferably a code editor like Sublime Text, Notepad++, VSCode, Vim, Emacs etc. Please DO NOT use a word processor like Word or Pages to open the config file.
- Configurations are in “key: value” format. There must be a space between colon and the value.
- api-url: keep it unchanged, unless you are using an environment other than Data Hub production environment
- token: paste in the API token saved in previous steps
- submission: paste in the submission ID saved in previous steps 
- type: must be set to “metadata”
- data: path to the folder that contains the metadata (TSV) files to be uploaded. All files with .txt or .tsv extensions inside the folder will be uploaded.
- intention: can be set to one of the following values: “New”, “Update”, “Delete”
- retries: number of retries the CLI tool will perform after a failed upload 
- overwrite: if set to “true”, CLI will upload a file to overwrite the file with same name that already exists in the Data Hub submission S3 bucket. If set to “false”, CLI will not upload a file if a file with the same name exists in the Data Hub submission S3 bucket.
- dryrun: if set to “true”, CLI will not upload any files to the Data Hub submission S3 bucket. If set to “false”, CLI will upload files to the Data Hub submission S3 bucket.

### Execute upload command

Depends on how Python3 was installed, on some systems you need to use “python” instead of “python3” in following command.

`$ python3 src/uploader.py --config configs/metadata-upload.yml`

