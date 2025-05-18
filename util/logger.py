import logging
import os
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from util.logger_config import LOG_FILE_LEVEL, LOG_CONSOLE_LEVEL

# 日志文件存放目录
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # 如果目录不存在则创建

# 需要单独输出的日志级别（高到低，便于优先处理高优先级日志）
LOG_LEVELS = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]

# logger缓存，避免重复创建
_loggers = {}

# 全局formatter缓存，避免重复创建
_formatter = logging.Formatter(
    '[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class LevelFilter(logging.Filter):
    """
    日志级别过滤器，只允许大于等于指定级别的日志通过。
    这样可以保证每个日志文件只包含对应级别及以上的日志。
    """

    def __init__(self, min_level):
        super().__init__()
        self.min_level = min_level  # 最低日志级别

    def filter(self, record):
        # 只允许大于等于min_level的日志记录
        return record.levelno >= self.min_level


def get_logger(name: str, level=logging.INFO):
    """
    获取或创建一个带有多级别文件和控制台输出的logger。
    - name: logger名称（通常为模块名）
    - level: logger的最低级别（一般不用手动设置，使用配置文件）
    """
    if name in _loggers:
        return _loggers[name]
    logger = logging.getLogger(name)
    # 避免重复添加handler
    if logger.hasHandlers():
        logger.handlers.clear()
    file_level = getattr(logging, LOG_FILE_LEVEL.upper(), logging.INFO)  # 文件日志级别
    console_level = getattr(logging, LOG_CONSOLE_LEVEL.upper(), logging.INFO)  # 控制台日志级别
    # logger的最低级别，只有高于此级别的日志才会被处理
    logger.setLevel(min(file_level, console_level))
    # 为每个日志级别分别创建文件handler
    for log_level in LOG_LEVELS:
        level_name = logging.getLevelName(log_level).lower()  # 级别名小写
        log_file = os.path.join(LOG_DIR, f"{level_name}.log")  # 日志文件名
        # 每天生成一个新文件，保留7天
        time_handler = TimedRotatingFileHandler(
            log_file, when='midnight', backupCount=7, encoding='utf-8'
        )
        time_handler.setLevel(log_level)
        time_handler.setFormatter(_formatter)
        time_handler.addFilter(LevelFilter(log_level))  # 只记录对应级别及以上日志
        logger.addHandler(time_handler)

    # 控制台输出handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(_formatter)
    logger.addHandler(console_handler)
    logger.propagate = False  # 防止日志重复输出到root logger
    _loggers[name] = logger  # 缓存logger
    return logger 