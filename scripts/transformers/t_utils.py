from html.parser import HTMLParser


class MyHTMLParser(HTMLParser):
    """ Used to decode html text"""

    def __init__(self):
        HTMLParser.__init__(self)
        self.reset()
        self.HTMLDATA = []

    def handle_starttag(self, tag, attrs):
        pass

    def handle_endtag(self, tag):
        pass

    def handle_data(self, data):
        self.HTMLDATA = data


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


def cast_bool(val):
    if val in ("false", "False", False, 'No'):
        return False
    elif val in ("True", "true", True, 'Sí'):
        return True
    elif not val:
        return None
