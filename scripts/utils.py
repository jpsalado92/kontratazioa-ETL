import logging
import time
from datetime import date


def get_current_year() -> int:
    return date.today().year


def separate_duplicates(items):
    """
    Given a list of objects, retrieves:
        A list of the set of the duplicated objects.
        A list of the set of the not duplicated objects.
    """

    # If there are no duplicates, return empty list and original list
    if len(items) == len(set(items)):
        return [], items

    # If there are duplicates, return a list containing repeated items and another one with single items
    else:
        visited = set()
        dup_set = {x for x in items if x in visited or (visited.add(x) or False)}
        single_set = set(items) - dup_set
        return list(dup_set), list(single_set)


def del_none(d: dict):
    """
    Delete keys with the `null` value in a dictionary, recursively.
    """
    for key, value in list(d.items()):

        if value in ("None", "", None, "Null", "null", "none"):
            del d[key]

        elif isinstance(value, dict):
            del_none(value)

        elif isinstance(value, list):
            for element in value:
                if isinstance(element, dict):
                    del_none(element)
    return d


def retry(times, exceptions, sleep=5):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    """

    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 1
            while attempt < times + 1:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logging.warning(f'Exception thrown when attempting to run "{func.__name__}",'
                                    f'attempt {attempt} of {times} with kwargs: {kwargs}')
                    time.sleep(sleep)
                    attempt += 1
            return 0

        return newfn

    return decorator


def strip_dict(d):
    try:
        for k, v in d.items():
            d[k] = str(v).strip().removesuffix('.')
    except AttributeError:
        raise
    return d


def check_no_matched_key(cont_d, known_keys):
    """ Raises an exception if unknown keys are found in the dict object """
    if not all([k in known_keys for k in cont_d]):
        unknown_keys = set(cont_d.keys()) - set(known_keys)
        raise KeyError(f"The following concepts are not under known cont-keys: {unknown_keys}")


def clean_xml_text(text: str):
    """ Format values according to their possible data type """

    # Clean line breaks and leading or ending spaces
    text = text.strip().replace('\n', '').strip()

    # If empty string return None
    if not text:
        return None

    # Handle integers and numbers
    try:
        if '_' not in text:
            if '.' in text:
                return float(text)
            else:
                return int(text)
    except:
        pass

    # Handle boolean values
    if text == "FALSE":
        return False
    elif text == "TRUE":
        return True

    return text


def parse_xml_field(node, path='', dict_obj=None, list_fields=[], pref2remove=[]):
    """
    Given a `CONT` `.xml` nested object, recursively returns a plain dict object
    """
    if dict_obj is None:
        dict_obj = {}
    try:
        node_text = clean_xml_text(node.text)
    except:
        node_text = None

    node_tag = node.tag
    for pref in pref2remove:
        node_tag = node_tag.replace(pref, '')

    if path:
        new_path = '-'.join((path, node_tag))
    else:
        new_path = node_tag

    if node_tag in list_fields:
        container = []
        for child in node:
            dd = {}
            parse_xml_field(child, '', dd, list_fields, pref2remove)
            container.append(dd.copy())
        dict_obj[new_path] = container.copy()

    else:
        if node_text:
            if new_path in dict_obj:
                print(new_path)
                raise
            else:
                dict_obj[new_path] = node_text

        for child in node:
            parse_xml_field(child, new_path, dict_obj, list_fields, pref2remove)

    return dict_obj


def get_key(dict_obj, possible_keys):
    for key in possible_keys:
        if dict_obj.get(key):
            return dict_obj[key]
    return None


def to_caps_string(text):
    return str(text).upper().strip()


if __name__ == "__main__":
    print(type(get_current_year()))
