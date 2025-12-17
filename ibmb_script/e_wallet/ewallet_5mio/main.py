from utils.logger import logger
from utils.flag import flag_non_qr_5mio

job_title = "E-wallet 5 mio"


def main():
    logger.info(f"{job_title} Process starting...")
    flag_non_qr_5mio()
    logger.info(f"{job_title} IDR completed.")


if __name__ == "__main__":
    main()
