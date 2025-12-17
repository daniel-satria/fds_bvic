from utils.logger import logger
from utils.historical import update_historical
from utils.daily import update_flag_10min, update_flag_5mio


def main():
    # Historical
    logger.info("="*50)
    logger.info("STEP 1: HISTORICAL PROCESS")
    logger.info("="*50)
    logger.info("Running QR Historical Process...")
    update_historical()
    logger.info("Historical QR Historical Process succeeded.")

    # 10min
    logger.info("="*50)
    logger.info("STEP 2: UPDATE FLAG 10min PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 10min Process...")
    update_flag_10min()
    logger.info("Update Flag 10min Process succeeded.")

    # 50mio
    logger.info("="*50)
    logger.info("STEP 3: UPDATE FLAG 5mio PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 5mio Process...")
    update_flag_5mio()
    logger.info("Update Flag 5mio Process succeeded.")


if __name__ == "__main__":
    main()
