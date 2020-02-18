from logging import handlers
import logging
import config

# log settings
formatter = logging.Formatter('%(asctime)s,%(message)s')

# handler settings
logHandler = handlers.TimedRotatingFileHandler(
    filename=config.log_path, when='midnight', interval=1, encoding='utf-8')
logHandler.setFormatter(formatter)
logHandler.suffix = "%Y%m%d"

# logger set
logger = logging.getLogger()
logger.setLevel(logging.WARN)
logger.addHandler(logHandler)
