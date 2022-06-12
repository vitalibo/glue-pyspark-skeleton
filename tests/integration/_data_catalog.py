import os
import tempfile
import time
from abc import ABC, abstractmethod
from urllib.parse import urlparse

import pytest
from pyspark.sql import DataFrame


class Table(ABC):
    """ Abstract class that represents physical table """

    def __init__(self, spark, helpers):
        self._spark = spark
        self._helpers = helpers

    def from_resource(self, root, path, schema, *args, **kwargs) -> None:
        """ Upload data from a resource into the table """

        df = self._spark \
            .create_dataframe_from_resource(root, path, schema, *args, **kwargs) \
            .repartition(1) \
            .cache()

        self.from_dataframe(df)

    def assert_equals(self, root, path, schema, *args, **kwargs) -> None:
        """ Check that the data from resources is equals to table """

        actual = self.as_dataframe()
        expected = self._spark.create_dataframe_from_resource(root, path, schema, *args, **kwargs)
        self._helpers.assert_dataframe_equals(actual, expected, *args, **kwargs)

    @abstractmethod
    def from_dataframe(self, df: DataFrame) -> None:
        """ Upload data from DataFrame into the table """

    @abstractmethod
    def as_dataframe(self) -> DataFrame:
        """ Read data from the table as DataFrame """

    @abstractmethod
    def truncate(self):
        """ Remove integration tests activity """


class AthenaTable(Table):
    """ Table proxy for Amazon Athena table """

    def __init__(self, spark, helpers, s3, athena, glue, database, name):  # pylint: disable=too-many-arguments
        super().__init__(spark, helpers)
        self._s3 = s3
        self._athena = athena
        self._descriptor = glue.get_table(DatabaseName=database, Name=name)['Table']

    def from_dataframe(self, df: DataFrame) -> None:
        """ Upload DataFrame to S3 """

        location = urlparse(self.storage_descriptor['Location'])
        params = self.parameters
        format = params['classification']  # noqa pylint: disable=redefined-builtin

        with tempfile.TemporaryDirectory() as path:
            writer = df.write

            partition_keys = [col['Name'] for col in self.partition_keys]
            if partition_keys:
                writer = writer \
                    .partitionBy(*partition_keys)

            if format == 'csv':
                writer = writer \
                    .option('sep', params.get('delimiter')) \
                    .option('header', int(params.get('skip.header.line.count', 0)) > 0)

            writer.save(
                path=path,
                mode='overwrite',
                format=format,
                compression=params.get('compressionType', 'none'))

            for file in [os.path.join(parent, file) for parent, _, files in os.walk(path) for file in files]:
                if file.endswith('_SUCCESS') or file.endswith('.crc'):
                    continue

                self._s3.meta.client.upload_file(file, location.netloc, location.path[1:] + file[len(path) + 1:])

    def as_dataframe(self) -> DataFrame:
        """ Execute Amazon Athena query and return as DataFrame """

        columns = {
            column['Name']: column['Type']
            for group in [self.storage_descriptor['Columns'], self.partition_keys] for column in group
        }

        s3_bucket, s3_path = self._execute_query(
            f'SELECT {{cols}} FROM "{self.database_name}"."{self.name}"'.format(cols=','.join(columns.keys())))
        filename = os.path.join(tempfile.gettempdir(), os.path.basename(s3_path))
        self._s3.meta.client.download_file(s3_bucket, s3_path, filename)

        schema = self._helpers.json_as_struct_type({
            'fields': [
                {
                    'name': cname,
                    'type': {
                        'boolean': 'boolean',
                        'bigint': 'long',
                        'int': 'integer',
                        'string': 'string',
                        'timestamp': 'string',
                        'double': 'double',
                        'float': 'float'
                    }.get(ctype, 'string')
                } for cname, ctype in columns.items()
            ]
        })

        return self._spark \
            .spark_session.read \
            .csv(filename, schema=schema, quote='"', escape='"', header=True) \
            .cache()

    def truncate(self):
        """ Clean Up S3 bucket """

        location = urlparse(self.storage_descriptor['Location'])
        bucket = self._s3.Bucket(location.netloc)
        bucket.objects.filter(Prefix=location.path[1:]).delete()

    def _execute_query(self, query):
        response = self._athena.start_query_execution(QueryString=query, WorkGroup='primary')
        execution_id = response['QueryExecutionId']

        state = 'QUEUED'
        while state in ('QUEUED', 'RUNNING'):
            response = self._athena.get_query_execution(QueryExecutionId=execution_id)
            state = response['QueryExecution']['Status']['State']
            time.sleep(5)

        if state in ('FAILED', 'CANCELLED'):
            reason = response['QueryExecution']['Status']['StateChangeReason']
            raise AthenaExecutionException(reason)

        s3_path = urlparse(response['QueryExecution']['ResultConfiguration']['OutputLocation'])
        return s3_path.netloc, s3_path.path[1:]

    def __getattr__(self, item):
        camel_case = ''.join(word.title() for word in item.split('_'))
        if camel_case in self._descriptor:
            return self._descriptor[camel_case]
        return None


class AthenaExecutionException(Exception):
    """ Raises when Amazon Athena query finished with non-success state """


@pytest.fixture(scope='function', name='data_catalog')
def data_catalog_fixture(spark, helpers, boto3_session, infra_props):
    with DataCatalog(spark, helpers, boto3_session, infra_props) as data_catalog:
        yield data_catalog


class DataCatalog:
    """ DataCatalog allows to create instances of table """

    def __init__(self, spark, helpers, boto3_session, infra_props):
        self._spark = spark
        self._helpers = helpers
        self._boto3_session = boto3_session
        self._infra_props = infra_props
        self._instances = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for table in self._instances.values():
            table.truncate()

    def athena(self, name) -> Table:
        """ Creates an instance of Amazon Athena table """

        if name not in self._instances:
            self._instances[name] = AthenaTable(
                self._spark,
                self._helpers,
                self._boto3_session.resource('s3'),
                self._boto3_session.client('athena'),
                self._boto3_session.client('glue'),
                self._infra_props['data_catalog'],
                name)

        return self._instances[name]
