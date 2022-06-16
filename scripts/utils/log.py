import logging
import os
import sys
from datetime import datetime


def start_log(path):
    # Enable logging and avoid urllib3 related logging
    logs_path = os.path.join(path, f'{datetime.now().strftime("%Y%m%d%H%M%S")}_event_log.txt')
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(module)-15s || [%(levelname)s] - %(message)s',
        datefmt='%Y/%d/%m %H:%M:%S',
        handlers=[
            logging.FileHandler(logs_path, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def start_end(func):
    def wrapper(*args, **kwargs):
        logging.info("Start: " + func.__name__)
        value = func(*args, **kwargs)
        logging.info("End: " + func.__name__)
        return value

    return wrapper
