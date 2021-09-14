import logging.config
import json
import os


def get_logger(logger_name: str = 'root'):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(dir_path, 'logging.json')
    if os.path.exists(path):
        with open(path, "r") as f:
            config = json.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)
    return logging.getLogger(logger_name)
