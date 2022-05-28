from unittest import mock

import pytest

from dp import driver
from dp.core.spark import Spark, Job


def test_main():
    with mock.patch('dp.infra.factory.create_spark') as mock_create_spark, \
            mock.patch('dp.infra.factory.create_job') as mock_create_job:
        mock_spark = mock.MagicMock(spec=Spark)
        mock_create_spark.return_value = mock_spark
        mock_spark.__enter__.return_value = mock_spark
        mock_job = mock.Mock(spec=Job)
        mock_create_job.return_value = mock_job
        argv = ['--foo=bar']

        driver.main(argv)

        mock_create_spark.assert_called_once_with(argv)
        mock_create_job.assert_called_once_with(argv)
        mock_spark.__enter__.assert_called_once()
        mock_spark.submit.assert_called_once_with(mock_job)
        mock_spark.__exit__.assert_called_once()


def test_main_create_job_failure():
    with mock.patch('dp.infra.factory.create_spark') as mock_create_spark, \
            mock.patch('dp.infra.factory.create_job') as mock_create_job:
        mock_spark = mock.MagicMock(spec=Spark)
        mock_create_spark.return_value = mock_spark
        mock_spark.__enter__.return_value = mock_spark
        mock_create_job.side_effect = Exception()
        argv = ['--foo=bar']

        with pytest.raises(Exception) as _:
            driver.main(argv)

        mock_create_spark.assert_not_called()
        mock_create_job.assert_called_once_with(argv)
        mock_spark.__enter__.assert_not_called()
        mock_spark.submit.assert_not_called()
        mock_spark.__exit__.assert_not_called()


def test_main_create_spark_failure():
    with mock.patch('dp.infra.factory.create_spark') as mock_create_spark, \
            mock.patch('dp.infra.factory.create_job') as mock_create_job:
        mock_spark = mock.MagicMock(spec=Spark)
        mock_create_spark.side_effect = Exception()
        mock_spark.__enter__.return_value = mock_spark
        mock_job = mock.Mock(spec=Job)
        mock_create_job.return_value = mock_job
        argv = ['--foo=bar']

        with pytest.raises(Exception) as _:
            driver.main(argv)

        mock_create_spark.assert_called_once_with(argv)
        mock_create_job.assert_called_once_with(argv)
        mock_spark.__enter__.assert_not_called()
        mock_spark.submit.assert_not_called()
        mock_spark.__exit__.assert_not_called()


def test_main_submit_failure():
    with mock.patch('dp.infra.factory.create_spark') as mock_create_spark, \
            mock.patch('dp.infra.factory.create_job') as mock_create_job:
        mock_spark = mock.MagicMock(spec=Spark)
        mock_create_spark.return_value = mock_spark
        mock_spark.__enter__.return_value = mock_spark
        mock_spark.submit.side_effect = Exception()
        mock_job = mock.Mock(spec=Job)
        mock_create_job.return_value = mock_job
        argv = ['--foo=bar']

        with pytest.raises(Exception) as _:
            driver.main(argv)

        mock_create_spark.assert_called_once_with(argv)
        mock_create_job.assert_called_once_with(argv)
        mock_spark.__enter__.assert_called_once()
        mock_spark.submit.assert_called_once_with(mock_job)
        mock_spark.__exit__.assert_called_once()
