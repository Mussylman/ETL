# log_setup.py
import logging
import os
from datetime import datetime

def setup_logging(log_directory):
    today = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"import_orders_{today}.log"
    log_filepath = os.path.join(log_directory, log_filename)

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    return log_filepath
