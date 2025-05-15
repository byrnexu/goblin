import logging
import os
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_LEVELS = [logging.INFO, logging.WARNING, logging.ERROR, logging.DEBUG]

_loggers = {}

def get_logger(name: str, level=logging.INFO):
    if name in _loggers:
        return _loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    for log_level in LOG_LEVELS:
        level_name = logging.getLevelName(log_level).lower()
        log_file = os.path.join(LOG_DIR, f"{level_name}.log")
        # 每天一个文件
        time_handler = TimedRotatingFileHandler(
            log_file, when='midnight', backupCount=7, encoding='utf-8'
        )
        time_handler.setLevel(log_level)
        time_handler.setFormatter(formatter)
        # 单文件超100MB切分
        size_handler = RotatingFileHandler(
            log_file, maxBytes=100*1024*1024, backupCount=10, encoding='utf-8'
        )
        size_handler.setLevel(log_level)
        size_handler.setFormatter(formatter)
        logger.addHandler(time_handler)
        logger.addHandler(size_handler)
    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    logger.propagate = False
    _loggers[name] = logger
    return logger 