from datetime import datetime, date, timezone

import pytest

from conftest import Helpers


def test_create_source_from_resource(spark):
    source = spark.create_source_from_resource(__file__, 'data/data_frame.json', 'data/data_frame_schema.json')

    actual = source.extract(spark)

    assert actual.toJSON().collect() == [
        '{"col1":"foo","col2":1,"col3":"2022-06-01T12:34:56.789Z"}',
        '{"col1":"bar","col2":2,"col4":"2022-06-02"}'
    ]


def test_create_sink_from_resource(spark):
    sink = spark.create_sink_from_resource(__file__, 'data/data_frame.json', 'data/data_frame_schema.json')
    df = spark.create_data_frame([
        {'col1': 'foo', 'col2': 1, 'col3': datetime(2022, 6, 1, 12, 34, 56, 789000, tzinfo=timezone.utc)},
        {'col1': 'bar', 'col2': 2, 'col4': date(2022, 6, 2)}
    ])

    sink.load(spark, df)


def test_create_dataframe_from_resource(spark):
    actual = spark.create_dataframe_from_resource(__file__, 'data/data_frame.json', 'data/data_frame_schema.json')

    assert actual.count() == 2
    assert actual.toJSON().collect() == [
        '{"col1":"foo","col2":1,"col3":"2022-06-01T12:34:56.789Z"}',
        '{"col1":"bar","col2":2,"col4":"2022-06-02"}'
    ]
    assert actual.schema['col1'].dataType.typeName() == 'string'
    assert actual.schema['col2'].dataType.typeName() == 'long'
    assert actual.schema['col3'].dataType.typeName() == 'timestamp'
    assert actual.schema['col4'].dataType.typeName() == 'date'


def test_resource():
    actual = Helpers.resource(__file__, 'foo.json')

    assert actual.endswith('src/test/python/unit/foo.json')


def test_resource_as_str():
    actual = Helpers.resource_as_str(__file__, 'data/resource.txt')

    assert actual == 'foo\n'


def test_resource_as_json():
    actual = Helpers.resource_as_json(__file__, 'data/resource.json')

    assert actual == {'foo': 'bar'}


def test_resource_as_json_str():
    actual = Helpers.resource_as_json_str(__file__, 'data/resource.json')

    assert actual == '{"foo": "bar"}'


def test_assert_dataframe_equals(spark):
    df1 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])
    df2 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])

    Helpers.assert_dataframe_equals(df1, df2)


def test_assert_dataframe_no_equals(spark):
    df1 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])
    df2 = spark.create_data_frame([{'col1': 'baz', 'col2': 1}, {'col1': 'bar', 'col2': 3}])

    with pytest.raises(AssertionError):
        Helpers.assert_dataframe_equals(df1, df2)


def test_assert_dataframe_equals_diff_orders(spark):
    df1 = spark.create_data_frame([{'col1': 'bar', 'col2': 2}, {'col1': 'foo', 'col2': 1}])
    df2 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])

    with pytest.raises(AssertionError):
        Helpers.assert_dataframe_equals(df1, df2)
    Helpers.assert_dataframe_equals(df1, df2, order_by=['col2'])


def test_assert_dataframe_equals_diff_schemas(spark):
    df1 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])
    df1 = df1.withColumn('col2', df1.col2.cast('double'))
    df2 = spark.create_data_frame([{'col1': 'foo', 'col2': 1}, {'col1': 'bar', 'col2': 2}])

    with pytest.raises(AssertionError):
        Helpers.assert_dataframe_equals(df1, df2)
    Helpers.assert_dataframe_equals(df1, df2, ignore_schema=True)


def test_json_as_struct_type():
    actual = Helpers.json_as_struct_type(
        Helpers.resource_as_json(__file__, 'data/struct_type.json'))

    assert actual['col1'].dataType.typeName() == 'byte'
    assert not actual['col1'].nullable
    assert actual['col1'].metadata == {'foo': 'bar'}
    assert actual['col2'].dataType.typeName() == 'short'
    assert actual['col2'].nullable
    assert actual['col2'].metadata == {}
    assert actual['col3'].dataType.typeName() == 'integer'
    assert actual['col4'].dataType.typeName() == 'long'
    assert actual['col5'].dataType.typeName() == 'float'
    assert actual['col6'].dataType.typeName() == 'double'
    assert actual['col7'].dataType.typeName() == 'string'
    assert actual['col8'].dataType.typeName() == 'boolean'
    assert actual['col9'].dataType.typeName() == 'timestamp'
    assert actual['col10'].dataType.typeName() == 'date'
    assert actual['col11'].dataType.simpleString() == 'array<string>'
    assert actual['col12'].dataType.simpleString() == 'struct<f1:string,f2:int>'
    assert actual['col13'].dataType.simpleString() == 'map<string,int>'


def test_mark_cases():
    actual = pytest.mark.cases('foo', 'bar')

    assert isinstance(actual, pytest.MarkDecorator)
    assert actual.mark.args == ('case', [pytest.param('case0', id='foo'), pytest.param('case1', id='bar')])
