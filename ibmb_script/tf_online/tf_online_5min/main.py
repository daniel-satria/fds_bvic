from utils.logger import logger
from utils.flag import flag_tf_online_5min


def main():
    logger.info("Transfer Online 5 Minutes Process starting...")
    flag_tf_online_5min()
    logger.info("Transfer Online 5 Minutes Process completed.")


if __name__ == "__main__":
    main()
