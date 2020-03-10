import datetime
import os
from itertools import chain, starmap


def dict_compare(old_dict, new_dict, nested=None):
    """ Compare two dictionaries
        Only 1 level, ignoring attributes starting with '_'
    """
    key_prefix = nested + '|' if nested else ''
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


# def buid_dict(composed_key, key_value):
#     def recursive_buid_dict(keys, value, nest_level=0):
#         if nest_level < len(keys):
#             return recursive_buid_dict(keys, value={keys[nest_level]: value}, nest_level=nest_level + 1)
#         elif nest_level == len(keys):
#             return value
#
#     return recursive_buid_dict(keys=list(reversed(composed_key.split('.'))), value=key_value)
#
# from collections import ChainMap
#
# def merge_dicts(dict_a, dict_b):
#     new_dict = dict()
#     for key in ChainMap(dict_a, dict_b):
#         val_a , val_b = dict_a.get(key), dict_b.get(key)
#         if isinstance(val_a, dict) and isinstance(val_b, dict):
#             new_dict[key] = merge_dicts(val_a, val_b)
#         elif val_a is None and val_b is None:
#             continue
#         elif val_a is None:
#             new_dict[key] = val_b
#         elif val_b is None or val_a == val_b:
#             new_dict[key] = val_a
#         else:
#             new_dict[key] = [val_a, val_b]
#     return new_dict

class Compare(object):

    def __init__(self, old_dictionary=None, new_dictionary=None, ignore_starting=None, ignore_ending=None):
        self.old_dict = old_dictionary
        self.new_dict = new_dictionary
        self.ignore_starting = ignore_starting
        self.ignore_ending = ignore_ending
        self.result = self.deepcompare(old_dict=old_dictionary,
                                       new_dict=new_dictionary,
                                       ignore_starting=self.ignore_starting,
                                       ignore_ending=self.ignore_starting)
        self.flatresult = self.flat()

    @staticmethod
    def intersect_lists(left_list, right_list):
        """Intersect two list and return difference as tuple (left, middle, right)

        USAGE:  
            l, _, _ = intersect_list(left_list, right_list)
                return list of values in left list not matched with right list
            
            _, m, _ = intersect_list(left_list, right_list)
                return list of values in matched in left list and right list
            
            _, _, r = intersect_list(left_list, right_list)
                return list of values in right list not matched with left list
            
            l, m, r = intersect_list(left_list, right_list)
                return all three above mentioned results

        EXAMPLE:
            l, m, r = intersect_list([1, 2, 3], [2, 3, 4])
            l = [1]
            m = [2, 3]
            r = [4]
        """
        l = [value for value in left_list if value not in right_list]
        m = [value for value in left_list if value in right_list]
        r = [value for value in right_list if value not in left_list]
        return l, m, r

    @staticmethod
    def unnest(dictionary, separator='.'):
        """Flatten a nested dictionary structure
        
        EXAMPLE:
        test_unnest = {'a': {'b': {'c': {'d': ['e', 'f'], 'g': 5}}}}
        unnest(test_unnest)
            returns {'a.b.c.d': ['e', 'f'], 'a.b.c.g': 5}
        """

        def unpack(parent_key, parent_value):
            """Unpack one level of nesting in a dictionary"""
            try:
                for key, value in parent_value.items():
                    yield (f'{parent_key}{separator}{key}', value)
            except AttributeError:
                # parent_value was not a dict, no need to flatten
                yield (f'{parent_key}', parent_value)

        if not isinstance(dictionary, dict):
            return dictionary
        while any(isinstance(value, dict) for value in dictionary.values()): # TODO secure breaking
            # Keep unpacking the dictionary until all value's are not dictionary's
            try:
                dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
            except AttributeError:
                break
        return dictionary

    @staticmethod
    def exclude_keys(set_of_keys, starting=None, ending=None):
        """Remove keys from set of keys starting and/or ending with string
        :param set_of_keys: a set of string keys to be iterated for ignoring keys
        :param starting: keys starting with string to be removed from a set of keys (default None)
        :param ending: keys ending with string to be removed from a set of keys (default None)
        """

        try:
            clean_keys = set()
            for key in set_of_keys:
                if starting and ending and key.startswith(starting) and key.endswith(ending):
                    continue
                elif starting and key.startswith(starting):
                    continue
                elif ending and key.endswith(ending):
                    continue
                else:
                    clean_keys.add(key)
            return clean_keys
        except TypeError as te:
            raise te

    def listtodict(self, lst):
        """Convert list of dictionaries to dictionary of dictionaries using index of list
        converts list of objects to dictionary of objects
        :param lst: list or dictionary to be converted to dictionary of objects
        """
        #NOTE: if object is not a dictionary it will be converted using default key as parent and index as child key
        if not isinstance(lst, (list, dict)):
            return lst
        try:
            result = {key: self.listtodict(value) for key, value in lst.items()}
        except AttributeError:
            result = {f'{index}': self.listtodict(value) for index, value in enumerate(lst)}
        return result

    def deepcompare(self, old_dict, new_dict, ignore_starting=None, ignore_ending=None):
        """Compare nested dictionaries, including lists recursively
        :param old_dict: the dictionary to be compared with
        :param new_dict: the dictionary to compare 
        :param ignore_starting: keys to be ignored starting with string (default None)
        :param ignore_ending: keys to be ignored ending with string (default None)
        :param unnested_result: boolean if return unnest(normalized) result
        :param separator: if unnested_result separator used to normalize keys
        """
        if old_dict == new_dict:
            return
        result = {}
        try:
            old = self.exclude_keys(old_dict.keys(), ignore_starting, ignore_ending)
            new = self.exclude_keys(new_dict.keys(), ignore_starting, ignore_ending)
            overlapping_keys = old & new
            old_keys = old - new
            new_keys = new - old
            if old_keys:
                for key in old_keys:
                    result.update({key: {'action': 'del', 'old': old_dict[key]}})
            if new_keys:
                for key in new_keys:
                    result.update({key: {'action': 'add', 'new': new_dict[key]}})
            for key in overlapping_keys:
                if isinstance(old_dict[key], list) and isinstance(new_dict[key], list):
                    old_d = self.listtodict(old_dict[key])
                    new_d = self.listtodict(new_dict[key])
                    value = self.deepcompare(old_d, new_d)
                    if value is None:
                        continue
                else:
                    value = self.deepcompare(old_dict[key], new_dict[key])
                if value is None:
                    continue
                if all(f'{index}' == key for index, key in enumerate(sorted(value.keys()))):
                    value = [v for _, v in value.items()]
                result.update({key: value})
        except AttributeError:
            if old_dict != new_dict:
                result = {'action': 'mod', 'old': old_dict, 'new': new_dict}
        finally:
            if result:
                return result

    def flat(self, dictionary=None, separator='.', unpack_lists=False):
        if unpack_lists == True:
            return self.unnest(self.listtodict(dictionary or self.result), separator=separator)
        else:
            return self.unnest(dictionary or self.result, separator=separator)