
import sys

""" 
clean_up_key_value(dict)
Removes leading and trailing spaces from keys and values in a dictionary
:param dict:
:return: cleaned dict
"""   
def clean_up_key_value(dict):
        
    return {key if not key else key.strip() if isinstance(key, str) else key : 
            value if not value else value.strip() if isinstance(value, str) else value for key, value in dict.items()}

"""
Removes leading and trailing spaces from header names
:param headers:
:return:
"""
def clean_up_strs(str_arr):
       
    return [item.strip() for item in str_arr]

def get_exception_msg():
    ex_type, ex_value, exc_traceback = sys.exc_info()
    return f'{ex_type.__name__}: {ex_value}'
