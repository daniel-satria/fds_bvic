from utils.logger import logger
from utils.flag import flag_tf_online_10min


def main():
    logger.info("Transfer Online 10 Minutes Process starting...")
    flag_tf_online_10min()
    logger.info("Transfer Online 10 Minutes Process completed.")


if __name__ == "__main__":
    main()
