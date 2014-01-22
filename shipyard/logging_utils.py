import logging

def setup_logging():
    logger = logging.getLogger()

    # Clear any old handlers
    old_handlers = logger.handlers
    for handler in old_handlers:
        logger.removeHandler(handler)

    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(message)s', "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger