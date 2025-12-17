from utils.logger import logger
from utils.flag import flag_qr_10min


def main():
    logger.info("QR 10 Minutes Process starting...")
    flag_qr_10min()
    logger.info("QR 10 Minutes Process completed.")


if __name__ == "__main__":
    main()
