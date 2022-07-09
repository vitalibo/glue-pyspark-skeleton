import sys

import awsglue
from awsglue import job, context
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import DataFrame

from dp.core.spark import Spark, Source, Sink


class GlueSpark(Spark):
    """ Extend Spark with AWS Glue ETL functionality """

    def __init__(self, sc: SparkContext):
        self.__gc = context.GlueContext(sc)
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


class GlueSource(Source):
    """ Allows to create Source from AWS Glue connection options """

    def __init__(self, **kwargs):
        self.__kwargs = kwargs

    def extract(self, spark: GlueSpark, *args, **kwargs) -> DataFrame:
        dyf = spark.glue_context \
            .create_dynamic_frame_from_options(
                connection_type=self.__kwargs['connection_type'],
                connection_options=self.__kwargs.get('connection_options', {}),
                format=self.__kwargs['format'],
                format_options=self.__kwargs.get('format_options', {}),
                push_down_predicate=kwargs.get('push_down_predicate', ''),
                transformation_ctx=kwargs.get('transformation_ctx') or self.__kwargs.get('transformation_ctx', ''))

        return dyf.toDF()


class GlueSink(Sink):
    """ Allows to create Sink from AWS Glue connection options """

    def __init__(self, **kwargs):
        self.__kwargs = kwargs

    def load(self, spark: GlueSpark, df: DataFrame, *args, **kwargs) -> None:
        transformation_ctx = kwargs.get('transformation_ctx') or self.__kwargs.get('transformation_ctx', '')

        spark.glue_context \
            .write_dynamic_frame \
            .from_options(
                frame=awsglue.DynamicFrame.fromDF(df, spark.glue_context, transformation_ctx),
                connection_type=self.__kwargs['connection_type'],
                connection_options=self.__kwargs.get('connection_options', {}),
                format=self.__kwargs['format'],
                format_options=self.__kwargs.get('format_options', {}),
                transformation_ctx=transformation_ctx)
