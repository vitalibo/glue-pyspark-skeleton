import importlib
from unittest import mock

import pytest

from dp import driver
from dp.core.spark import Spark, Job
from dp.infra.factory import Factory


@mock.patch('dp.infra.factory.Factory')
def test_main(mock_factory_constructor):
    mock_factory = mock.Mock(spec=Factory)
    mock_factory_constructor.return_value = mock_factory
    importlib.reload(driver)
    mock_spark = mock.MagicMock(spec=Spark)
    mock_factory.create_spark.return_value = mock_spark
    mock_spark.__enter__.return_value = mock_spark
    mock_job = mock.Mock(spec=Job)
    mock_factory.create_job.return_value = mock_job
    argv = ['--foo=bar']

    driver.main(argv)

    mock_factory_constructor.assert_called_once_with(argv)
    mock_factory.create_spark.assert_called_once()
    mock_factory.create_job.assert_called_once()
    mock_spark.__enter__.assert_called_once()
    mock_spark.submit.assert_called_once_with(mock_job)
    mock_spark.__exit__.assert_called_once()


@mock.patch('dp.infra.factory.Factory')
def test_main_create_job_failure(mock_factory_constructor):
    mock_factory = mock.Mock(spec=Factory)
    mock_factory_constructor.return_value = mock_factory
    importlib.reload(driver)
    mock_spark = mock.MagicMock(spec=Spark)
    mock_factory.create_spark.return_value = mock_spark
    mock_spark.__enter__.return_value = mock_spark
    mock_factory.create_job.side_effect = Exception()
    argv = ['--foo=bar']

    with pytest.raises(Exception) as _:
        driver.main(argv)

    mock_factory_constructor.assert_called_once()
    mock_factory.create_spark.assert_not_called()
    mock_factory.create_job.assert_called_once()
    mock_spark.__enter__.assert_not_called()
    mock_spark.submit.assert_not_called()
    mock_spark.__exit__.assert_not_called()


@mock.patch('dp.infra.factory.Factory')
def test_main_create_spark_failure(mock_factory_constructor):
    mock_factory = mock.Mock(spec=Factory)
    mock_factory_constructor.return_value = mock_factory
    importlib.reload(driver)
    mock_spark = mock.MagicMock(spec=Spark)
    mock_factory.create_spark.side_effect = Exception()
    mock_spark.__enter__.return_value = mock_spark
    mock_job = mock.Mock(spec=Job)
    mock_factory.create_job.return_value = mock_job
    argv = ['--foo=bar']

    with pytest.raises(Exception) as _:
        driver.main(argv)

    mock_factory_constructor.assert_called_once_with(argv)
    mock_factory.create_spark.assert_called_once()
    mock_factory.create_job.assert_called_once()
    mock_spark.__enter__.assert_not_called()
    mock_spark.submit.assert_not_called()
    mock_spark.__exit__.assert_not_called()


@mock.patch('dp.infra.factory.Factory')
def test_main_submit_failure(mock_factory_constructor):
    mock_factory = mock.Mock(spec=Factory)
    mock_factory_constructor.return_value = mock_factory
    importlib.reload(driver)
    mock_spark = mock.MagicMock(spec=Spark)
    mock_factory.create_spark.return_value = mock_spark
    mock_spark.__enter__.return_value = mock_spark
    mock_spark.submit.side_effect = Exception()
    mock_job = mock.Mock(spec=Job)
    mock_factory.create_job.return_value = mock_job
    argv = ['--foo=bar']

    with pytest.raises(Exception) as _:
        driver.main(argv)

    mock_factory_constructor.assert_called_once_with(argv)
    mock_factory.create_spark.assert_called_once()
    mock_factory.create_job.assert_called_once()
    mock_spark.__enter__.assert_called_once()
    mock_spark.submit.assert_called_once_with(mock_job)
    mock_spark.__exit__.assert_called_once()
