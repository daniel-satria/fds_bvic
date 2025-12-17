from utils.logger import logger
from utils.flag import flag_qr_10min


job_title = "E-wallet 10min"


def main():
    logger.info(f"{job_title} Process starting...")
    flag_qr_10min()
    logger.info(f"{job_title} Process completed.")


if __name__ == "__main__":
    main()
