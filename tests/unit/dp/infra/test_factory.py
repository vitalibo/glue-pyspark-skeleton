import sys
from unittest import mock

import pytest

from dp.core.spark import Job, Spark
from dp.infra.factory import Factory


# pylint: disable=protected-access
def test_create_spark():
    mock_factory = mock.Mock()
    mock_factory._conf = {'properties': {'foo': '123'}}
    mock_glue_spark = mock.Mock()
    with mock.patch('dp.infra.aws.glue.GlueSpark') as mock_glue_spark_class:
        mock_glue_spark_class.return_value = mock_glue_spark
        actual = Factory.create_spark(mock_factory)

        assert actual == mock_glue_spark
        mock_glue_spark_class.assert_called_once()
        sc = mock_glue_spark_class.call_args[0][0]
        assert not sc._jsc.sc().isStopped()
        conf = sc.getConf()
        assert conf.get('spark.sql.session.timeZone') == 'UTC'
        assert conf.get('foo') == '123'


def test_create_job():
    mock_factory = mock.Mock()
    mock_factory._parse_kwargs.return_value = {'arg1': '123', 'arg2': 'qwerty'}
    mock_factory._job_name = 'embedded_test_job'
    mock_factory._conf = {'kwargs': {'foo': 123}}
    with mock.patch('importlib.import_module') as mock_import_module:
        mock_import_module.return_value = sys.modules[__name__]

        actual = Factory.create_job(mock_factory)

        assert isinstance(actual, EmbeddedTestJob)
        assert actual.arg1 == '123'
        assert actual.arg2 == 'qwerty'
        mock_factory._parse_kwargs.assert_called_once_with({'kwargs': {'foo': 123}})


def test_create_job_unknown_args():
    mock_factory = mock.Mock()
    mock_factory._parse_kwargs.return_value = {'arg1': '123', 'arg2': 'qwerty', 'arg3': 'foo'}
    mock_factory._job_name = 'embedded_test_job'
    with mock.patch('importlib.import_module') as mock_import_module:
        mock_import_module.return_value = sys.modules[__name__]

        with pytest.raises(TypeError) as _:
            Factory.create_job(mock_factory)


def test_create_source():
    pass


def test_create_sink():
    pass


def test_create_decorator():
    mock_factory = mock.Mock()
    mock_factory._parse_kwargs.return_value = {'arg1': '123', 'arg2': 'qwerty'}
    config = {
        'class': 'unit.dp.infra.test_factory.EmbeddedTestDecorator',
        'kwargs': {
            'foo': 321
        }
    }

    actual = Factory._create_decorator(mock_factory, 'dummy', config)

    assert isinstance(actual, EmbeddedTestDecorator)
    assert actual.arg1 == '123'
    assert actual.arg2 == 'qwerty'
    mock_factory._parse_kwargs.assert_called_once_with(config)


def test_parse_kwargs():
    mock_factory = mock.Mock()
    mock_factory._create_type1.return_value = 'abc'
    mock_factory._create_type2.return_value = {'a': 'b'}
    config = {
        'kwargs': {
            'foo': 123,
            'bar': {'__create': 'type1'},
            'baz': {'__create': 'type2'},
            'tar': {'far': 123}
        }
    }

    actual = Factory._parse_kwargs(mock_factory, config)

    mock_factory._create_type1.assert_called_once_with('bar', {'__create': 'type1'})
    mock_factory._create_type2.assert_called_once_with('baz', {'__create': 'type2'})
    assert actual == {
        'foo': 123,
        'bar': 'abc',
        'baz': {'a': 'b'},
        'tar': {'far': 123}
    }


@pytest.mark.parametrize('sample, expected', [
    ('foo-job', 'foo_job'),
    ('vb-dev-foo-job', 'foo_job'),
    ('vb-pre-dev-foobar-job', 'foobar_job'),
    ('etl-prod-foo-bar-job', 'foo_bar_job'),
    # pylint: disable-next=fixme
    ('etl-prod-bar-foo-job', 'foo_job')  # FIXME: should be bar_foo_job ???
])
def test_parse_job_name(sample, expected):
    argv = ['./test.py', f'--JOB_NAME={sample}', '--bar=baz']
    conf = {'foo_job': '', 'foo_bar_job': '', 'foobar_job': '', 'bar_foo_job': ''}

    actual = Factory._parse_job_name(argv, conf)

    assert actual == expected


def test_parse_job_name_no_recognized():
    argv = ['./test.py', '--JOB_NAME=tar-job', '--bar=baz']
    conf = {'foo_job': '', 'foo_bar_job': '', 'foobar_job': '', 'bar_foo_job': ''}

    with pytest.raises(ValueError) as _:
        Factory._parse_job_name(argv, conf)


def test_parse_argv():
    actual = Factory._parse_argv(['./test.py', '--foo=123', '--bar=baz'], 'foo')

    assert actual == '123'


def test_parse_argv_missed():
    with pytest.raises(ValueError) as _:
        Factory._parse_argv(['./test.py', '--foo=123', '--bar=baz'], 'tag')


def test_parse_argv_with_dash():
    actual = Factory._parse_argv(['./test.py', '--foo=123', '--bar-tar=baz'], 'bar-tar')

    assert actual == 'baz'


def test_parse_conf():
    data = """
foo: 123
bar: baz
tar:
  far: false
  bar: abc
---
bar: new
tar:
  bar: ABC
  a:
    b: 0
baz: qwerty
tor:
  - 12
  - 23
    """
    with mock.patch('builtins.open', mock.mock_open(read_data=data)) as mock_open_read:
        actual = Factory._parse_conf('app.yaml')

        mock_open_read.assert_called_once_with('app.yaml', 'r', encoding='utf-8')
        assert actual == {
            'foo': 123,
            'bar': 'new',
            'tar': {
                'far': False,
                'bar': 'ABC',
                'a': {'b': 0}
            },
            'baz': 'qwerty',
            'tor': [12, 23]
        }


class EmbeddedTestDecorator:  # pylint: disable=too-few-public-methods
    """ Internal class used for tests """

    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2


class EmbeddedTestJob(Job):
    """ Internal class used for tests """

    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def transform(self, spark: Spark) -> None:
        pass
