import hashlib
import logging
import time
from datetime import date


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
            logging.warning(f'Unable to succesfully run "{func.__name__}" after {times} attempts. Params: ({kwargs})')
            return 0

        return newfn

    return decorator


def get_current_year() -> int:
    return date.today().year


def get_hash(s):
    h = hashlib.sha3_512()
    h.update(bytes(s, 'utf-8'))
    return h.hexdigest()


def flatten(xss):
    return [x for xs in xss for x in xs]
