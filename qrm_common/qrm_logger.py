import logging
import os
from datetime import datetime


def init_logger(app_name: str,
                file_log_level: int = logging.DEBUG,
                console_log_level: int = logging.INFO,
                max_log_size_in_megabyte: int = 10,
                log_formatter: str = '[%(asctime)s] [%(module)s] [%(name)s] [%(levelname)s] %(message)s') -> None:

    date_dir = datetime.now().strftime('%Y-%m')
    log_dir_path = os.path.join('/var', 'log', app_name, date_dir)
    log_file_name = f'{app_name}.txt'
    full_log_file_path = os.path.join(log_dir_path, log_file_name)
    print(f'log file path {full_log_file_path}')
    os.makedirs(log_dir_path, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(filename=full_log_file_path,
                                              maxBytes=max_log_size_in_megabyte * 1024,
                                              backupCount=10)
    fh.setLevel(file_log_level)
    ch = logging.StreamHandler()
    ch.setLevel(console_log_level)
    formatter = logging.Formatter(log_formatter)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logging.info(f'log file path {full_log_file_path}')
