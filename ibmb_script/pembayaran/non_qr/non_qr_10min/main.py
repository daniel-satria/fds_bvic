from utils.logger import logger
from utils.flag import flag_non_qr_10min


def main():
    logger.info("Transfer Online 5 Minutes Process starting...")
    flag_non_qr_10min()
    logger.info("Transfer Online 5 Minutes Process completed.")


if __name__ == "__main__":
    main()
