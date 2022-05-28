from unittest import mock

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

from dp.spark import Spark, Job, Source, Sink


def test_submit():
    spark = Spark(mock.Mock(spec=SparkSession))
    mock_job = mock.Mock(spec=Job)

    spark.submit(mock_job)

    mock_job.transform.assert_called_with(spark)


def test_extract():
    spark = Spark(mock.Mock(spec=SparkSession))
    mock_source = mock.Mock(spec=Source)
    mock_dataframe = mock.Mock(spec=DataFrame)
    mock_source.extract.return_value = mock_dataframe

    actual = spark.extract(mock_source, 123, foo='bar')

    assert actual == mock_dataframe
    mock_source.extract.assert_called_with(spark, 123, foo='bar')


def test_load():
    spark = Spark(mock.Mock(spec=SparkSession))
    mock_sink = mock.Mock(spec=Sink)
    mock_dataframe = mock.Mock(spec=DataFrame)

    spark.load(mock_sink, mock_dataframe, 123, foo='bar')

    mock_sink.load.assert_called_with(spark, mock_dataframe, 123, foo='bar')


def test_create_data_frame():
    mock_spark_session = mock.Mock(spec=SparkSession)
    spark = Spark(mock_spark_session)
    mock_dataframe = mock.Mock(spec=DataFrame)
    mock_spark_session.createDataFrame.return_value = mock_dataframe

    actual = spark.create_data_frame(123, foo='bar')

    assert actual == mock_dataframe
    mock_spark_session.createDataFrame.assert_called_with(123, foo='bar')


def test_spark_session():
    mock_spark_session = mock.Mock(spec=SparkSession)
    spark = Spark(mock_spark_session)

    actual = spark.spark_session

    assert actual == mock_spark_session


def test_spark_context():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session.sparkContext = mock_spark_context
    spark = Spark(mock_spark_session)

    actual = spark.spark_context

    assert actual == mock_spark_context


def test_spark_conf():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_conf = mock.Mock(spec=SparkConf)
    mock_spark_session.sparkContext = mock_spark_context
    mock_spark_context.getConf.return_value = mock_spark_conf
    spark = Spark(mock_spark_session)

    actual = spark.spark_conf

    assert actual == mock_spark_conf


def test_executor_instances():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_conf = mock.Mock(spec=SparkConf)
    mock_spark_session.sparkContext = mock_spark_context
    mock_spark_context.getConf.return_value = mock_spark_conf
    mock_spark_conf.get.return_value = 3
    spark = Spark(mock_spark_session)

    actual = spark.executor_instances

    assert actual == 3
    mock_spark_conf.get.assert_called_with('spark.executor.instances', '1')


def test_executor_cores():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_conf = mock.Mock(spec=SparkConf)
    mock_spark_session.sparkContext = mock_spark_context
    mock_spark_context.getConf.return_value = mock_spark_conf
    mock_spark_conf.get.return_value = 3
    spark = Spark(mock_spark_session)

    actual = spark.executor_cores

    assert actual == 3
    mock_spark_conf.get.assert_called_once_with('spark.executor.cores', '1')


def test_total_cores():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_conf = mock.Mock(spec=SparkConf)
    mock_spark_session.sparkContext = mock_spark_context
    mock_spark_context.getConf.return_value = mock_spark_conf
    mock_spark_conf.get.side_effect = [3, 2]
    spark = Spark(mock_spark_session)

    actual = spark.total_cores

    assert actual == 6
    assert mock_spark_conf.get.call_args_list == \
           [mock.call('spark.executor.instances', '1'), mock.call('spark.executor.cores', '1')]


def test_stop_spark_context():
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session.sparkContext = mock_spark_context
    spark = Spark(mock_spark_session)

    with spark as actual:
        assert spark == actual
        mock_spark_context.stop.assert_not_called()
    mock_spark_context.stop.assert_called_once()
