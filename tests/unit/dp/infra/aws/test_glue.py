from unittest import mock

import pytest
from awsglue import job, DynamicFrame
from awsglue.context import GlueContext
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame

from dp.infra.aws.glue import GlueSpark, GlueSource, GlueSink


@mock.patch('awsglue.job.Job')
@mock.patch('awsglue.context.GlueContext')
def test_glue_spark_context(mock_glue_context_constructor, mock_glue_job_constructor):
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_glue_context_constructor.return_value = mock_glue_context
    mock_job = mock.Mock(spec=job.Job)
    mock_glue_job_constructor.return_value = mock_job
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_glue_context.spark_session = mock_spark_session
    spark = GlueSpark(mock_spark_context)

    actual = spark.glue_context

    assert actual == mock_glue_context
    mock_glue_context_constructor.assert_called_with(mock_spark_context)
    mock_glue_job_constructor.assert_called_with(mock_glue_context)


@mock.patch('awsglue.job.Job')
@mock.patch('awsglue.context.GlueContext')
def test_glue_spark_job(mock_glue_context_constructor, mock_glue_job_constructor):
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_glue_context_constructor.return_value = mock_glue_context
    mock_job = mock.Mock(spec=job.Job)
    mock_glue_job_constructor.return_value = mock_job
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_glue_context.spark_session = mock_spark_session
    spark = GlueSpark(mock_spark_context)

    actual = spark.glue_job

    assert actual == mock_job
    mock_glue_context_constructor.assert_called_with(mock_spark_context)
    mock_glue_job_constructor.assert_called_with(mock_glue_context)


@mock.patch('awsglue.job.Job')
@mock.patch('awsglue.context.GlueContext')
def test_glue_spark_context_manager(mock_glue_context_constructor, mock_glue_job_constructor):
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_glue_context_constructor.return_value = mock_glue_context
    mock_job = mock.Mock()
    mock_glue_job_constructor.return_value = mock_job
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_glue_context.spark_session = mock_spark_session
    mock_spark_session.sparkContext = mock_spark_context
    spark = GlueSpark(mock_spark_context)

    with mock.patch('sys.argv', ['./test.py', '--JOB_NAME=test-job']):
        with spark as actual:
            assert actual == spark
            mock_job.commit.assert_not_called()

    assert mock_job.init.call_args[0][0] == 'test-job'
    mock_job.commit.assert_called_once()
    mock_spark_context.stop.assert_called_once()


@mock.patch('awsglue.job.Job')
@mock.patch('awsglue.context.GlueContext')
def test_glue_spark_context_manager_when_failure(mock_glue_context_constructor, mock_glue_job_constructor):
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_glue_context_constructor.return_value = mock_glue_context
    mock_job = mock.Mock()
    mock_glue_job_constructor.return_value = mock_job
    mock_spark_context = mock.Mock(spec=SparkContext)
    mock_spark_session = mock.Mock(spec=SparkSession)
    mock_glue_context.spark_session = mock_spark_session
    mock_spark_session.sparkContext = mock_spark_context
    spark = GlueSpark(mock_spark_context)

    with mock.patch('sys.argv', ['./test.py', '--JOB_NAME=test-job']):
        with pytest.raises(Exception), spark:
            raise Exception('foo')

    assert mock_job.init.call_args[0][0] == 'test-job'
    mock_job.commit.assert_not_called()
    mock_spark_context.stop.assert_called_once()


def test_glue_source_extract():
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_data_frame = mock.Mock(spec=DataFrame)
    mock_dynamic_frame.toDF.return_value = mock_data_frame
    mock_spark_glue.glue_context = mock_glue_context
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_dynamic_frame
    source = GlueSource(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'}
    )

    actual = source.extract(mock_spark_glue)

    assert actual == mock_data_frame
    mock_glue_context.create_dynamic_frame_from_options.assert_called_once_with(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        push_down_predicate='',
        transformation_ctx=''
    )


def test_glue_source_extract_with_transformation_ctx():
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_data_frame = mock.Mock(spec=DataFrame)
    mock_dynamic_frame.toDF.return_value = mock_data_frame
    mock_spark_glue.glue_context = mock_glue_context
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_dynamic_frame
    source = GlueSource(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='foo'
    )

    actual = source.extract(mock_spark_glue)

    assert actual == mock_data_frame
    mock_glue_context.create_dynamic_frame_from_options.assert_called_once_with(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        push_down_predicate='',
        transformation_ctx='foo'
    )


def test_glue_source_extract_override():
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_data_frame = mock.Mock(spec=DataFrame)
    mock_dynamic_frame.toDF.return_value = mock_data_frame
    mock_spark_glue.glue_context = mock_glue_context
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_dynamic_frame
    source = GlueSource(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='foo'
    )

    actual = source.extract(
        mock_spark_glue,
        push_down_predicate='bar > 10',
        transformation_ctx='baz'
    )

    assert actual == mock_data_frame
    mock_glue_context.create_dynamic_frame_from_options.assert_called_once_with(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        push_down_predicate='bar > 10',
        transformation_ctx='baz'
    )


@mock.patch('awsglue.DynamicFrame')
def test_glue_sink_load(mock_dynamic_frame_class):
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_spark_glue.glue_context = mock_glue_context
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_dynamic_frame_class.fromDF.return_value = mock_dynamic_frame
    mock_write_dynamic_frame = mock.Mock()
    mock_glue_context.write_dynamic_frame = mock_write_dynamic_frame
    mock_data_frame = mock.Mock(spec=DataFrame)
    sink = GlueSink(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'}
    )

    sink.load(mock_spark_glue, mock_data_frame)
    mock_dynamic_frame_class.fromDF.assert_called_with(mock_data_frame, mock_glue_context, '')
    mock_write_dynamic_frame.from_options.assert_called_once_with(
        frame=mock_dynamic_frame,
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx=''
    )


@mock.patch('awsglue.DynamicFrame')
def test_glue_sink_load_with_transformation_ctx(mock_dynamic_frame_class):
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_spark_glue.glue_context = mock_glue_context
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_dynamic_frame_class.fromDF.return_value = mock_dynamic_frame
    mock_write_dynamic_frame = mock.Mock()
    mock_glue_context.write_dynamic_frame = mock_write_dynamic_frame
    mock_data_frame = mock.Mock(spec=DataFrame)
    sink = GlueSink(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='foo'
    )

    sink.load(mock_spark_glue, mock_data_frame)
    mock_dynamic_frame_class.fromDF.assert_called_with(mock_data_frame, mock_glue_context, 'foo')
    mock_write_dynamic_frame.from_options.assert_called_once_with(
        frame=mock_dynamic_frame,
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='foo'
    )


@mock.patch('awsglue.DynamicFrame')
def test_glue_sink_load_override(mock_dynamic_frame_class):
    mock_spark_glue = mock.Mock(spec=GlueSpark)
    mock_glue_context = mock.Mock(spec=GlueContext)
    mock_spark_glue.glue_context = mock_glue_context
    mock_dynamic_frame = mock.Mock(spec=DynamicFrame)
    mock_dynamic_frame_class.fromDF.return_value = mock_dynamic_frame
    mock_write_dynamic_frame = mock.Mock()
    mock_glue_context.write_dynamic_frame = mock_write_dynamic_frame
    mock_data_frame = mock.Mock(spec=DataFrame)
    sink = GlueSink(
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='foo'
    )

    sink.load(mock_spark_glue, mock_data_frame, transformation_ctx='bar')
    mock_dynamic_frame_class.fromDF.assert_called_with(mock_data_frame, mock_glue_context, 'bar')
    mock_write_dynamic_frame.from_options.assert_called_once_with(
        frame=mock_dynamic_frame,
        connection_type='s3',
        connection_options={'path': 's3://foo/bar/'},
        format='csv',
        format_options={'separator': '\t'},
        transformation_ctx='bar'
    )
