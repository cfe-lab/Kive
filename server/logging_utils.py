import logging

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def set_logging_function_name(function_name):
    logger = logging.getLogger()
    for h in logger.handlers:
        logger.removeHandler(h)
    formatter = logging.Formatter('%(asctime)s - {} - %(message)s'.format(function_name))
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)