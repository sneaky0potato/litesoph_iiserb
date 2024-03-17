import subprocess

def execute_cmd(cmd, dir):
    proc = subprocess.Popen(cmd, cwd=dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc

def keys_exists(_dict, keys):
    """function to check if keys exist or not in a dictionary
    
    parameter:

    dictionary: dictionary on which keys needs to be check
    keys: list of keys
    
    """
    for key in keys:
        if not key in _dict:
            return False
    return True