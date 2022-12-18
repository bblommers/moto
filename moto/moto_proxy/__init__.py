import logging
import sys


log_format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(stream=sys.stdout, format=log_format)
logger = logging.getLogger("MOTO_PROXY")
logger.setLevel(logging.INFO)


def with_color(color: int, text: str):
    return f"\x1b[{color}m{text}\x1b[0m"


def info(msg):
    logger.info(msg)


def debug(msg):
    logger.debug(msg)


def error(msg):
    logger.error(msg)
