from utils.logger import logger
from utils.historical import update_historical
from utils.daily import update_flag_5min, update_flag_10min, update_flag_50mio, update_flag_50mio_e


def main():
    # Historical
    logger.info("="*50)
    logger.info("STEP 1: HISTORICAL PROCESS")
    logger.info("="*50)
    logger.info("Running Historical Process...")
    update_historical()
    logger.info("Historical Process succeed.")

    # Update Flag 5 min
    logger.info("="*50)
    logger.info("STEP 2: UPDATE 5min FLAG PROCESS...")
    logger.info("="*50)
    logger.info("Running Update 5min Flag Process...")
    update_flag_5min()
    logger.info("Updating 5min flag Succeded.")

    # Update Flag 10 min
    logger.info("="*50)
    logger.info("STEP 3: UPDATE 10min FLAG PROCESS...")
    logger.info("="*50)
    logger.info("Running Update 10min flag Process...")
    update_flag_10min()
    logger.info("Updating 10min flag Succeded.")

    # Update Flag 50 mio
    logger.info("="*50)
    logger.info("STEP 4: UPDATE 50mio FLAG PROCESS...")
    logger.info("="*50)
    logger.info("Running Update 50mio flag Process...")
    update_flag_50mio()
    logger.info("Updating 50mio flag Succeded.")

    # Update Flag 50 mio early
    logger.info("="*50)
    logger.info("STEP 5: UPDATE 50mio EARLY FLAG PROCESS...")
    logger.info("="*50)
    logger.info("Running Update 50mio early flag Process...")
    update_flag_50mio_e()
    logger.info("Updating 50mio early flag Succeded.")

    logger.info("All processes finsihed.")


if __name__ == "__main__":
    main()
