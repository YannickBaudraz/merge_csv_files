import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = RotatingFileHandler(
        'app.log', maxBytes=5000000, backupCount=5)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)


def merge_csv_files(input_path):
    files = [f for f in os.listdir(input_path) if os.path.isfile(
        os.path.join(input_path, f))]
    logging.info(f"Found {len(files)} files in input directory.")

    data_frames = []
    for file_name in files:
        if not file_name.endswith('.csv'):
            logging.warning(f"Skipped non-CSV file: {file_name}")
            continue

        data = pd.read_csv(os.path.join(
            input_path, file_name), header=None, dtype=str)
        logging.info(f"Read data from {file_name} with {len(data)} rows.")
        data_frames.append(data)

    return pd.concat(data_frames, ignore_index=True)


def save_csv(df: pd.DataFrame, output: str):
    df.to_csv(output, index=False, header=False)
    logging.info(f"Data saved to {output}.")


def main():
    try:
        setup_logger()
        data_dir_path = os.path.dirname(os.path.realpath(__file__)) + "/data"
        input_path = os.path.join(data_dir_path, "in")
        logging.info(f"Input path set to {input_path}")

        save_csv(
            merge_csv_files(input_path),
            os.path.join(
                data_dir_path,
                'out',
                pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S") + '.csv'
            )
        )

    except Exception as e:
        logging.error("An error occurred", exc_info=True)


if __name__ == "__main__":
    main()
