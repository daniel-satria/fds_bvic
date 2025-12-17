from utils.logger import logger
from utils.flag import flag_non_qr_5mio


def main():
    logger.info("Non-QR 5 million IDR Process starting...")
    flag_non_qr_5mio()
    logger.info("Non-QR 5 million IDR completed.")


if __name__ == "__main__":
    main()
