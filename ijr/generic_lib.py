import datetime
import os


def dict_compare(old_dict, new_dict, nested=None):
    """ Compare two dictionaries
        Only 1 level, ignoring attributes starting with '_'
    """
    key_prefix = nested + '.' if nested else ''
    intersect_keys = old_dict.keys() & new_dict.keys()
    modified = {key_prefix + k: dict(old=old_dict[k], new=new_dict[k], action='mod') for k in intersect_keys
                        if k[0] != '_'
                        and old_dict[k] != new_dict[k]
                        and not (isinstance(old_dict[k], dict) and isinstance(new_dict[k], dict))}

    nested_keys = [k for k in intersect_keys if k[0] != '_' and isinstance(old_dict[k], dict) and isinstance(new_dict[k], dict)]
    for k in nested_keys:
        x = dict_compare(old_dict[k], new_dict[k], key_prefix + k)
        if x:
            modified.update(x)

    added = new_dict.keys() - old_dict.keys()
    modified.update({key_prefix + k: dict(new=new_dict[k], action='add') for k in added if k[0] != '_'})

    deleted = old_dict.keys() - new_dict.keys()
    modified.update({key_prefix + k: dict(old=old_dict[k], action='del') for k in deleted if k[0] != '_'})
    if modified:
        return modified


def running_in_gcf():
    """ Determine if code is running in GCF using X_GOOGLE_FUNCTION_NAME
    """
    return os.getenv('X_GOOGLE_FUNCTION_NAME') is not None


def default_object(o):
    """ Default handler for json.dumps()"""
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    raise TypeError("Type %s not serializable" % type(o))
