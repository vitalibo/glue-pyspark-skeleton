import logging
import sys
import traceback

from dp.infra import factory


def main(argv):
    """ Glue Job main entry point """

    try:
        job = factory.create_job(argv)

        with factory.create_spark(argv) as spark:
            spark.submit(job)

    except Exception as e:
        logging.fatal('Unexpected error', traceback.format_exc())
        raise e


if __name__ == '__main__':
    main(sys.argv)
