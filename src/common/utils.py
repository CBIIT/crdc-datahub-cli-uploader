
import sys
import csv
from uuid import UUID
from datetime import datetime
from common.constants import S3_START


""" 
clean_up_key_value(dict)
Removes leading and trailing spaces from keys and values in a dictionary
:param: dict as dictionary
:return: cleaned dict
"""   
def clean_up_key_value(dict):
        
    return {key if not key else key.strip() if isinstance(key, str) else key : 
            value if not value else value.strip() if isinstance(value, str) else value for key, value in dict.items()}

"""
Removes leading and trailing spaces from header names
:param: str_arr as str array
:return: cleaned str array
"""
def clean_up_strs(str_arr):
       
    return [item.strip() for item in str_arr]

"""
Extract exception type name and message
:return: str
"""
def get_exception_msg():
    ex_type, ex_value, exc_traceback = sys.exc_info()
    return f'{ex_type.__name__}: {ex_value}'


"""
Dump list of dictionary to TSV file, caller needs handle exception.
:param: dict_list as list of dictionary
:param: file_path as str
:return: boolean
"""
def dump_dict_to_tsv(dict_list, file_path):
    if not dict_list or len(dict_list) == 0:
        return False 
    keys = dict_list[0].keys()
    with open(file_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter='\t')
        dict_writer.writeheader()
        dict_writer.writerows(dict_list) 
    return True

def is_valid_uuid(uuid_to_test, version=5):
    """
    Check if uuid_to_test is a valid UUID.
    
     Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}
    
     Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.
    
     Examples
    --------
    >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
    True
    >>> is_valid_uuid('c9bf9e58')
    False
    """
    
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
        uuid_str = str(uuid_obj)
        if uuid_str == uuid_to_test:
            return True
        else:
            return is_valid_uuid(uuid_to_test, int(version)-1)    
    except ValueError:
        return False
    
def get_uuid(version=4):
    uuid = UUID(int=version)
    return str(uuid)

def get_datetime_str():
    return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def extract_s3_info_from_url(url):
    split_list = url.replace(S3_START, "").split("/")
    bucket_name = split_list[0]
    prefix = "/".join(split_list[1:])
    return bucket_name, prefix

def format_size(size_in_bytes):
    """
    Convert a size in bytes to a human-readable format with proper units.
    """
    # Define the units and their respective thresholds
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0

    # Convert to the appropriate unit
    while size_in_bytes >= 1024 and unit_index < len(units) - 1:
        size_in_bytes /= 1024
        unit_index += 1

    # Return the formatted string with 2 decimal places
    return f"{size_in_bytes:.2f} {units[unit_index]}"

def calculate_eclipse_time(file_size, upload_speed):
    """
    Calculate the upload time in hh:mm:ss format given the file size and upload speed.
    
    :param file_size: Size of the file in bytes.
    :param upload_speed: Upload speed in bps (megabits per second).
    :return: Upload time in hh:mm:ss format.
    """
    # Convert file size from bytes to bits (1 byte = 8 bits)
    file_size_bits = file_size * 8
    
    # Calculate total upload time in seconds
    upload_time_seconds = file_size_bits / upload_speed

    # Format the time as hh:mm:ss
    return format_time(upload_time_seconds)

def format_time(seconds):
    """
    Format seconds into hh:mm:ss format.

    :param seconds: Time in seconds.
    :return: Formatted time string in hh:mm:ss format.
    """
    if seconds < 1:
        return "less than 1 sec"
    hours = int(seconds // 3600)
    remaining_seconds = seconds % 3600
    minutes = int(remaining_seconds // 60)
    seconds = int(remaining_seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def dump_data_to_csv(dict_list, file_path):
    """
    save md5 cache to file
    """
    if not dict_list or len(dict_list) == 0:
        return 

    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=dict_list[0].keys())
        writer.writeheader()
        for row in dict_list:
            writer.writerow(row)

                           
    


