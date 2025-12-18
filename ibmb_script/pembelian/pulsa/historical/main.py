from utils.logger import logger
from utils.historical import update_historical
from utils.daily import update_flag_10min, update_flag_5mio

job_title = "PEMBAYARAN PULSA"


def main():
    # Historical
    logger.info("="*50)
    logger.info(f"STEP 1: HISTORICAL {job_title} PROCESS")
    logger.info("="*50)
    logger.info(f"Running {job_title} Historical Process...")
    update_historical()
    logger.info(f"Historical {job_title} Historical Process succeeded.")

    # 10min
    logger.info("="*50)
    logger.info(f"STEP 2: UPDATE FLAG {job_title} 10MIN PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 10min Process...")
    update_flag_10min()
    logger.info("Update Flag 10min Process succeeded.")

    # # 50mi
    logger.info("="*50)
    logger.info(f"STEP 3: UPDATE FLAG {job_title} 5MIO PROCESS")
    logger.info("="*50)
    logger.info("Running Update Flag 50mio Process...")
    update_flag_5mio()
    logger.info("Update Flag 50mio Process succeeded.")


if __name__ == "__main__":
    main()
