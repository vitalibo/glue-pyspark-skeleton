import logging
import sys

from dp.infra.factory import Factory


def main(argv):
    """ Glue Job main entry point """

    factory = Factory(argv)

    try:
        job = factory.create_job()

        with factory.create_spark() as spark:
            spark.submit(job)

    except Exception as e:
        logging.fatal('Unexpected error')
        raise e


if __name__ == '__main__':
    main(sys.argv)
