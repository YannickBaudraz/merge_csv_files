import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = RotatingFileHandler('app.log', maxBytes=5000000, backupCount=5)
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)

try:
    data_dir_path = os.path.dirname(os.path.realpath(__file__)) + "/data"
    input_path = data_dir_path + "/in"
    logging.info(f"Input path set to {input_path}")

    files = [f for f in os.listdir(input_path) if os.path.isfile(input_path + '/' + f)]
    logging.info(f"Found {len(files)} files in input directory.")

    df = pd.DataFrame()

    data_frames = []
    for file_name in files:
        if not file_name.endswith('.csv'):
            logging.warning(f"Skipped non-CSV file: {file_name}")
            continue

        data = pd.read_csv(input_path + '/' + file_name, header=None, dtype=str)
        logging.info(f"Read data from {file_name} with {len(data)} rows.")
        data_frames.append(data)

    df = pd.concat(data_frames, ignore_index=True)
    logging.info("Merged all csv files.")

    output_path = data_dir_path + '/out/' + str(pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")) + '.csv'
    df.to_csv(output_path, index=False, header=False)
    logging.info(f"Data saved to {output_path}.")

except Exception as e:
    logging.error("An error occurred", exc_info=True)
