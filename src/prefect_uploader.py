import os
import yaml
from uploader import controller
from prefect import flow
from bento.common.utils import get_logger, LOG_PREFIX, get_time_stamp
from common.constants import UPLOAD_TYPE, DRY_RUN, \
    CLI_VERSION, PRE_MANIFEST, API_URL, SUBMISSION_ID, TOKEN, RETRIES, OVERWRITE, FILE_DIR, TEMP_CONFIG_FILE

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Uploader Main'
log = get_logger('FileLoader')

@flow(name="prefect_cli_uploader", log_prints=True)
def prefect_uploader(submission: str, api_url: str, token: str, type: str, data: str, manifest: str,retries: int, overwrite: bool, dryrun: bool):
    try:
        # print cli version
        log.info(f"v{CLI_VERSION}")
        # download pre-manifest file from the s3 bucket
        configs = {}
        configs["Config"] = {PRE_MANIFEST: manifest, SUBMISSION_ID: submission, API_URL: api_url, FILE_DIR: data, TOKEN: token, UPLOAD_TYPE: type, DRY_RUN: dryrun, RETRIES: retries, OVERWRITE: overwrite}
        # save the configs to the temp config file
        with open(TEMP_CONFIG_FILE, 'w') as f:
            yaml.dump(configs, f)
        print(f"Saved the configs to the temp config file: {TEMP_CONFIG_FILE}")
        # run the controller with the temp config file as argument
        controller(TEMP_CONFIG_FILE)
    except Exception as e:
        log.error(f"Error in prefect_uploader: {e}")
        raise
    finally:
        # delete the temp config file
        if os.path.exists(TEMP_CONFIG_FILE):
            os.remove(TEMP_CONFIG_FILE)
            log.info(f"Deleted the temp config file: {TEMP_CONFIG_FILE}")
    
if __name__ == "__main__":
    prefect_uploader()