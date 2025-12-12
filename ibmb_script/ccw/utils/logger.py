import datetime
import logging
import os
import sys
import time


def jakarta(sec, when=None):
    '''Date & Time converter into GMT+7'''
    jakarta_time = datetime.datetime.now() + datetime.timedelta(hours=7)
    return jakarta_time.timetuple()


logging_str: str = "%(asctime)s: %(levelname)s: %(module)s: %(message)s"
log_dir: str = "logs"
log_date = time.strftime("%Y-%m-%d")
log_filename: str = f"{log_date}" + "_running_logs.log"
log_filepath: str = os.path.join(
    log_dir,
    log_filename
)

os.makedirs(log_dir,
            exist_ok=True
            )

logging.Formatter.converter = jakarta
logging.basicConfig(
    level=logging.DEBUG,
    format=logging_str,
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("CCW_Historical_Process_Logger")
