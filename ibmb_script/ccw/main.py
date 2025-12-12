from utils.logger import logger
from utils.daily import flag_ccw
from utils.historical import (
    update_historical,
    update_flag
)


def main():
    logger.info("CCW Process starting...")

    # Historical
    logger.info("="*50)
    logger.info("STEP 1: HISTORICAL PROCESS")
    logger.info("="*50)
    logger.info("Running Historical Process...")
    update_historical()
    logger.info("Historical Process succeed.")

    # Flagging CCW
    logger.info("="*50)
    logger.info("STEP 2: FLAG CCW PROCESS")
    logger.info("="*50)
    logger.info("Running Flag CCW Process...")
    flag_ccw()
    logger.info("Flag CCW Process completed.")

    # Update Flag
    logger.info("="*50)
    logger.info("STEP 3: UPDATE FLAG TO HISTORICAL DATA PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag Historical Process...")
    update_flag()
    logger.info("Update Flag Process succeed.")

    logger.info("CCW Process completed.")


if __name__ == "__main__":
    main()
