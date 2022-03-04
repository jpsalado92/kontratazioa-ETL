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
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    print(f'Exception thrown when attempting to run "{func.__name__}",'
                          f'attempt {attempt} of {times} with kwargs: {kwargs}')
                    time.sleep(sleep)
                    attempt += 1
            return func(*args, **kwargs)

        return newfn

    return decorator


def strip_dict(d):
    try:
        for k, v in d.items():
            d[k] = str(v).strip().removesuffix('.')
    except AttributeError:
        raise
    return d


if __name__ == "__main__":
    print(type(get_current_year()))
