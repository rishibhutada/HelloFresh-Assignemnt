import logging
import datetime

# import cal
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
currtime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
logger = ''


def init_logger(job_name, log_path):
    global logger
    log_file = "%s/%s_%s.log" % (log_path, job_name, currtime)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


def close_logger(logger):
    if len(logger.handlers) > 0:
        for handler in logger.handlers:
            logger.removeHandler(handler)
