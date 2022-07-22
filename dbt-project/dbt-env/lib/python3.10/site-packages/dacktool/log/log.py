import datetime
import logging


def log_error(error):
    logging.error("%s: %s" % (str(datetime.datetime.now()), error))


def log_info(info):
    logging.info("%s: %s" % (str(datetime.datetime.now()), info))
