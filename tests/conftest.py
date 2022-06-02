import json
import os
from operator import itemgetter

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from dp.core.spark import Spark, Source, Sink


@pytest.fixture(scope='module', name='spark')
def spark_fixture():
    """ Create instance embedded spark fixture for testing """

    session = SparkSession.builder \
        .appName('PyTest') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .getOrCreate()

    with EmbeddedSpark(session) as instance:
        yield instance


@pytest.fixture(name='helpers')
def helpers_fixture():
    """ Create helpers fixture for testing """

    return Helpers


class EmbeddedSpark(Spark):
    """ Decorator for Spark with additional functionality for testing """

    @staticmethod
    def create_source_from_resource(root, path, schema, *args, **kwargs) -> Source:
        """ Creates Source from resources for testing """

        class _InnerSource(Source):
            def extract(self, spark: EmbeddedSpark, *_args, **_kwargs) -> DataFrame:
                return spark.create_dataframe_from_resource(root, path, schema, *args, *_args, **kwargs, **_kwargs)

        return _InnerSource()

    @staticmethod
    def create_sink_from_resource(root, path, schema, *args, **kwargs) -> Sink:
        """ Creates Sink from resources for testing """

        class _InnerSink(Sink):
            def load(self, spark: EmbeddedSpark, df: DataFrame, *_args, **_kwargs) -> None:
                expected = spark.create_dataframe_from_resource(root, path, schema, *args, *_args, **kwargs, **_kwargs)
                Helpers.assert_dataframe_equals(df, expected, *args, *_args, **kwargs, **_kwargs)

        return _InnerSink()

    def create_dataframe_from_resource(self, root, path, schema, *args, **kwargs) -> DataFrame:
        """ Creates DataFrame from resources for testing """

        rdd = self \
            .spark_context \
            .wholeTextFiles(Helpers.resource(root, path), 1) \
            .map(itemgetter(1))

        df = self \
            .spark_session.read \
            .option('multiLine', True) \
            .schema(Helpers.resource_as_struct_type(root, schema)) \
            .json(rdd)

        return df


class Helpers:
    """ Test helper """

    @staticmethod
    def resource(root, path):
        """ Returns absolute path to resource """

        return os.path.join(os.path.dirname(root), path)

    @staticmethod
    def resource_as_str(root, path):
        """ Returns resource content as string """

        with open(Helpers.resource(root, path), 'r', encoding='utf-8') as file:
            return file.read()

    @staticmethod
    def resource_as_json(root, path):
        """ Returns resource as json object """

        return json.loads(Helpers.resource_as_str(root, path))

    @staticmethod
    def resource_as_json_str(root, path):
        """ Returns resource as json string """

        return json.dumps(Helpers.resource_as_json(root, path))

    @staticmethod
    def resource_as_struct_type(root, path):
        """ Returns resource as StructType """

        return Helpers.json_as_struct_type(
            Helpers.resource_as_json(root, path))

    @staticmethod
    def json_as_struct_type(schema):
        """ Returns StructType from Json object """

        def fill_nullable_fields(node):
            for field in node['fields']:
                if 'metadata' not in field:
                    field['metadata'] = {}
                if 'nullable' not in field:
                    field['nullable'] = True
                if isinstance(field['type'], dict) and 'type' in field['type']:
                    if field['type']['type'] == 'struct':
                        field['type'] = fill_nullable_fields(field['type'])
                    if field['type']['type'] == 'array' and 'type' in field['type']['elementType'] and \
                        field['type']['elementType']['type'] == 'struct':
                        field['type']['elementType'] = fill_nullable_fields(field['type']['elementType'])

            return node

        return StructType.fromJson(fill_nullable_fields(schema))

    @staticmethod
    def assert_dataframe_equals(actual, expected, *args, ignore_schema=False, order_by=None, **kwargs):
        """ Check that the DataFrames is equals """

        if order_by is not None:
            actual = actual.orderBy(*order_by)

        if not ignore_schema:
            assert actual.schema == expected.schema
        assert actual.collect() == expected.collect()
