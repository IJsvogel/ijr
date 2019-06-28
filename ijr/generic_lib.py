import os


def dict_compare(old_dict, new_dict):
    """ Compare two dictionaries
        Only 1 level, ignoring attributes starting with '_'
    """
    intersect_keys = old_dict.keys() & new_dict.keys()
    modified = {k: dict(old=old_dict[k], new=new_dict[k], action='mod')
                    for k in intersect_keys if k[0] != '_' and old_dict[k] != new_dict[k]}
    added = new_dict.keys() - old_dict.keys()
    modified.update({k: dict(new=new_dict[k], action='add') for k in added if k[0] != '_'})
    deleted = old_dict.keys() - new_dict.keys()
    modified.update({k: dict(old=old_dict[k], action='del') for k in deleted if k[0] != '_'})
    if modified:
        return modified


def running_in_gcf():
    """ Determine if code is running in GCF using X_GOOGLE_FUNCTION_NAME
    """
    return os.getenv('X_GOOGLE_FUNCTION_NAME') is not None
