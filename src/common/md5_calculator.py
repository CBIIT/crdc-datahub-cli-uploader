import hashlib
from common.progress_bar import create_progress_bar

# Constants
DEFAULT_CHUNK_SIZE = 1024*10  # Default chunk size for small files
LARGE_FILE_CHUNK_SIZE = 1024*1024  # 64 KB chunk size for large files

def calculate_file_md5(file_path, file_size, log):
    """
    Calculate the MD5 checksum of a file.
    Dynamically adjusts chunk size based on file size.
    Displays progress in a bar format.
    Returns md5_hash.
    """
    md5_hash = hashlib.md5()

    # Dynamically adjust chunk size based on file size
    if file_size > LARGE_FILE_CHUNK_SIZE:
        chunk_size = LARGE_FILE_CHUNK_SIZE
    else:
        chunk_size = DEFAULT_CHUNK_SIZE if file_size > DEFAULT_CHUNK_SIZE else file_size
    log.info(f'Start to calculate md5 of the data file, {file_path}...')
    try:
        with open(file_path, 'rb') as f, create_progress_bar() as progress:
            task = progress.add_task("Calculating MD5", total=file_size)
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5_hash.update(chunk)
                progress.update(task, advance=len(chunk))
            progress.update(task, completed=file_size)

    except Exception as e:
        print(f"An error occurred: {e}")
    return md5_hash.hexdigest()
