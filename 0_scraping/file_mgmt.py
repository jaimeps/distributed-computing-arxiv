import os

def check_dir(path):
    """
    check that path exists and is writable
    will raise an IOError exception if not writable
    :param path: path to check
    :return: True
    """
    try:
        os.stat(path)
    except:
        os.makedirs(path)

    if os.access(path, os.W_OK) == False:
        raise IOError("Error: not able to write to %s" % path)

    return True