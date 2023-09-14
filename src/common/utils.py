

""" 
_clean_up_key_value(dict)
Removes leading and trailing spaces from keys and values in a dictionary
:param dict:
:return: cleaned dict
"""   
@staticmethod
def _clean_up_key_value(dict):
        
        return {key if not key else key.strip(): value if not value else value.strip() for key, value in dict}