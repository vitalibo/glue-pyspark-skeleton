from pyspark.sql import DataFrame

from dp.core.spark import Spark, Source, Sink


# pylint: disable=protected-access
def func(name):
    """ Wraps Java transform """

    def wrap(df):
        class_name, func_name = name.split("::")
        cls = getattr(df._sc._jvm, class_name)
        jdf = getattr(cls, func_name)(df._jdf)
        return DataFrame(jdf, df.sql_ctx)

    return wrap


class JavaSource(Source):
    """ Wraps and creates instance on Java Source """

    def __init__(self, _class, *args):
        self._class = _class
        self._args = args

    def extract(self, spark: Spark, *args, **kwargs) -> DataFrame:
        cls = getattr(spark.spark_session._jvm, self._class)
        java_source = cls(*self._args)
        jdf = java_source.extract(spark.spark_session._jsparkSession)
        return DataFrame(jdf, spark.spark_session._wrapped)


class JavaSink(Sink):
    """ Wraps and creates instance on Java Sink """

    def __init__(self, _class, *args):
        self._class = _class
        self._args = args

    def load(self, spark: Spark, df: DataFrame, *args, **kwargs) -> None:
        cls = getattr(df._sc._jvm, self._class)
        java_sink = cls(*self._args)
        java_sink.load(spark.spark_session._jsparkSession, df._jdf)
