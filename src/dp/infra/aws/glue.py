import sys

from awsglue import job  # pylint: disable=import-error
from awsglue.context import GlueContext  # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from pyspark import SparkContext

from dp.core.spark import Spark


class GlueSpark(Spark):
    """ Extend Spark with AWS Glue ETL functionality """

    def __init__(self, sc: SparkContext):
        self.__gc = GlueContext(sc)
        self.__job = job.Job(self.__gc)
        super().__init__(self.__gc.spark_session)

    @property
    def glue_context(self) -> GlueContext:
        """ Returns the underlying :class:`awsglue.context.GlueContext` """

        return self.__gc

    @property
    def glue_job(self) -> job.Job:
        """ Returns the underlying :class:`awsglue.job.Job` """

        return self.__job

    def __enter__(self):
        super().__enter__()
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.__job.init(args['JOB_NAME'], args)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.__job.commit()

        super().__exit__(exc_type, exc_val, exc_tb)
