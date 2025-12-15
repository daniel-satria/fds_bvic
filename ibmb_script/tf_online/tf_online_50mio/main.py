from utils.logger import logger
from utils.flag import flag_tf_online_50mio


def main():
    logger.info("Transfer Online 50 million IDR Process starting...")
    flag_tf_online_50mio()
    logger.info("Transfer Online 50 million IDR completed.")


if __name__ == "__main__":
    main()
