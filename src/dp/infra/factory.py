from dp.core.spark import Spark, Job


class Factory:
    """ Factory class for all objects """

    def __init__(self, argv):
        self.__argv = argv
        self.__config = {}

    def create_spark(self) -> Spark:
        """ Create PySpark runner """

    def create_job(self) -> Job:
        """ Create instance of Job """
