from utils.logger import logger
from utils.flag import flag_5mio

job_title = "Flag Pembayaran Listrik 5mio"


def main():
    logger.info(f"{job_title} Process starting...")
    flag_5mio()
    logger.info(f"{job_title} IDR completed.")


if __name__ == "__main__":
    main()
