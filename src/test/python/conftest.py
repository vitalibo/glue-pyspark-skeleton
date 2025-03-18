import json
import os
import subprocess
from operator import itemgetter
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from dp.core.spark import Spark, Source, Sink
from integration._data_catalog import (  # noqa pylint: disable=unused-import
    data_catalog_fixture
)
from integration._infrastructure import (  # noqa pylint: disable=unused-import
    pytest_addoption,
    pytest_configure as infra_pytest_configure,
    pytest_collection_modifyitems as infra_pytest_collection_modifyitems,
    terraform_fixture,
    boto3_session_fixture,
    glue_job_executor_fixture
)

CLASS_PATH = Path(__file__).parents[3] / 'target' / 'classes'


@pytest.fixture(scope='session', name='spark')
def spark_fixture():
    """ Create instance embedded spark fixture for testing """

    session = SparkSession.builder \
        .appName('PyTest') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.extraClassPath', CLASS_PATH) \
        .config('spark.executor.extraClassPath', CLASS_PATH) \
        .getOrCreate()

    with EmbeddedSpark(session) as instance:
        yield instance


@pytest.fixture(scope='session', name='helpers')
def helpers_fixture():
    """ Create helpers fixture for testing """

    return Helpers


def marker_cases(*args):
    """ Decorator for parametrizing test functions """

    return pytest.mark.parametrize('case', [
        pytest.param(f'case{i}', id=item) for i, item in enumerate(args)
    ])


def pytest_configure(config):
    infra_pytest_configure(config)

    config.addinivalue_line('markers', 'mvn: mark a test that required compile java code.')


def pytest_collection_modifyitems(config, items):
    infra_pytest_collection_modifyitems(config, items)

    for item in items:
        if 'mvn' in item.keywords:
            mvn_compile()
            return


def mvn_compile():
    """ Compile Java source code """

    command = 'mvn clean compile -Dmaven.test.skip'
    with subprocess.Popen(args=command.split(), cwd=Path(__file__).parents[3]) as process:
        process.communicate()
        if process.returncode != 0:
            raise subprocess.SubprocessError('mvn failed')


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

        # This functionality allows reformat your DataFrame json file according to schema.
        #
        # WARNING: This code sample will override your original JSON file!
        #
        # formatted_json = json.dumps(json.loads(str(df.toJSON().collect()).replace("'", '')), indent=2)
        # with open(Helpers.resource(root, path), 'w', encoding='utf-8') as f:
        #     f.write(formatted_json + '\n')

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


pytest.mark.cases = marker_cases
