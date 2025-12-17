from utils.logger import logger
from utils.historical import update_historical
from utils.daily import update_flag_10min, update_flag_5mio


def main():
    # Historical
    logger.info("="*50)
    logger.info("STEP 1: HISTORICAL PEMBAYARAN NON-QR PROCESS")
    logger.info("="*50)
    logger.info("Running non-QR Historical Process...")
    update_historical()
    logger.info("Historical non-QR Historical Process succeeded.")

    # 10min
    logger.info("="*50)
    logger.info("STEP 2: UPDATE FLAG 10MIN PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 10min Process...")
    update_flag_10min()
    logger.info("Update Flag 10min Process succeeded.")

    # 50mio
    logger.info("="*50)
    logger.info("STEP 3: UPDATE FLAG 50MIO PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 50mio Process...")
    update_flag_5mio()
    logger.info("Update Flag 50mio Process succeeded.")


if __name__ == "__main__":
    main()
