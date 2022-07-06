from __future__ import annotations

import abc

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession


class Job(abc.ABC):
    """ Abstraction for Spark job """

    @abc.abstractmethod
    def transform(self, spark: Spark) -> None:
        """ Contains the high-level Apache Spark DSL transformations """


class Source(abc.ABC):
    """ Abstraction for external data source available for read """

    @abc.abstractmethod
    def extract(self, spark: Spark, *args, **kwargs) -> DataFrame:
        """ Creates a :class:`pyspark.sql.DataFrame` from external data storage """


class Sink(abc.ABC):
    """ Abstraction for external data source available for read """

    @abc.abstractmethod
    def load(self, spark: Spark, df: DataFrame, *args, **kwargs) -> None:
        """ Stores a :class:`pyspark.sql.DataFrame` into external data storage """


class Spark:
    """ The entry point to programming Apache Spark in terms Job, Source, Sink API """

    def __init__(self, session: SparkSession):
        self.__session = session
        self.__sc: SparkContext = session.sparkContext

    def submit(self, job: Job) -> None:
        """ Submit job into Apache Spark """

        job.transform(self)

    def extract(self, source: Source, *args, **kwargs) -> DataFrame:
        """ Creates a :class:`pyspark.sql.DataFrame` from external data storage """

        return source.extract(self, *args, **kwargs)

    def load(self, sink: Sink, df: DataFrame, *args, **kwargs) -> None:
        """ Stores a :class:`pyspark.sql.DataFrame` into external data storage """

        sink.load(self, df, *args, **kwargs)

    def create_data_frame(self, *args, **kwargs) -> DataFrame:
        """ Creates a :class:`pyspark.sql.DataFrame` from an :class:`pyspark.rdd.RDD` or a list """

        return self.__session.createDataFrame(*args, **kwargs)

    @property
    def spark_session(self) -> SparkSession:
        """ Returns the underlying :class:`pyspark.sql.SparkSession` """

        return self.__session

    @property
    def spark_context(self) -> SparkContext:
        """ Returns the underlying :class:`pyspark.SparkContext` """

        return self.__sc

    @property
    def spark_conf(self) -> SparkConf:
        """ Returns the underlying :class:`pyspark.SparkConf` """

        return self.__sc.getConf()

    @property
    def executor_instances(self) -> int:
        return int(self.spark_conf.get('spark.executor.instances', '1'))

    @property
    def executor_cores(self) -> int:
        return int(self.spark_conf.get('spark.executor.cores', '1'))

    @property
    def total_cores(self) -> int:
        return self.executor_instances * self.executor_cores

    def __enter__(self) -> Spark:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__sc.stop()
