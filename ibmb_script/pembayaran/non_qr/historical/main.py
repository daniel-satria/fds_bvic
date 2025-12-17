from utils.logger import logger
from utils.historical import update_historical


def main():
    # Historical
    logger.info("="*50)
    logger.info("HISTORICAL PROCESS")
    logger.info("="*50)
    logger.info("Running non-QR Historical Process...")
    update_historical()
    logger.info("Historical non-QR Historical Process succeed.")


if __name__ == "__main__":
    main()
