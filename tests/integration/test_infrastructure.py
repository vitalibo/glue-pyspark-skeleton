import subprocess
from unittest import mock

import pytest

from integration._infrastructure import TerraformProxy, GlueJobExecutor, GlueJobExecutionException


# pylint: disable=protected-access
@mock.patch('shutil.copyfile')
def test_terraform_apply(mock_copyfile):
    terraform = TerraformProxy('dev', 'my', '1q2w3e')
    terraform._update_tfvars = mock.Mock()
    terraform._execute = mock.Mock()
    terraform._execute.side_effect = [None, ('{"foo": {"value": 123}, "bar": {"value": "baz"}}', None)]

    actual = terraform.apply()

    assert actual == {'foo': 123, 'bar': 'baz'}
    args = mock_copyfile.call_args
    assert str(args[0][0]).endswith('infrastructure/vars/dev.tfvars')
    assert str(args[0][1]).endswith('infrastructure/vars/dev-1q2w3e.tfvars')
    terraform._update_tfvars.assert_called_with({'environment': '"%s-test"', 'name': '"1q2w3e-%s"'}, args[0][1])
    args = terraform._execute.call_args_list
    assert args[0][0][0] == 'apply'
    assert args[1][0][0] == 'output'
    assert args[1][1] == {'stdout': subprocess.PIPE}


@mock.patch('shutil.rmtree')
def test_terraform_destroy(mock_rmtree):
    terraform = TerraformProxy('dev', 'my', '1q2w3e')
    terraform._tfvars = mock.Mock()
    terraform._execute = mock.Mock()

    terraform.destroy()

    terraform._execute.assert_called_once_with('destroy')
    terraform._tfvars.unlink.assert_called_once()
    args = mock_rmtree.call_args
    assert str(args[0][0]).endswith('infrastructure/.terraform/dev-1q2w3e')


@mock.patch('subprocess.Popen')
def test_terraform_execute(mock_popen):
    terraform = TerraformProxy('dev', 'my', '1q2w3e')
    mock_subprocess = mock.MagicMock()
    mock_popen.return_value = mock_subprocess
    mock_subprocess.__enter__.return_value = mock_subprocess
    mock_subprocess.communicate.return_value = ('output', 'error')
    mock_subprocess.returncode = 0

    with mock.patch('time.time') as mock_time:
        mock_time.return_value = 12.34
        actual = terraform._execute('apply')

    assert actual == ('output', 'error')
    mock_subprocess.communicate.assert_called_once()
    mock_subprocess.__enter__.assert_called_once()
    args = mock_popen.call_args
    assert args[1]['args'] == 'make apply --no-print-directory ' \
                              'environment=dev-1q2w3e profile=my auto_approve=true version=12.34'.split(' ')
    assert str(args[1]['cwd']).endswith('/infrastructure')


@mock.patch('subprocess.Popen')
def test_terraform_execute_with_error(mock_popen):
    terraform = TerraformProxy('dev', 'my', '1q2w3e')
    mock_subprocess = mock.MagicMock()
    mock_popen.return_value = mock_subprocess
    mock_subprocess.__enter__.return_value = mock_subprocess
    mock_subprocess.communicate.return_value = ('output', 'error')
    mock_subprocess.returncode = 1

    with pytest.raises(subprocess.SubprocessError):
        terraform._execute('apply')


@mock.patch('builtins.open', new_callable=mock.mock_open())
@mock.patch('sys.stdout.write')
@mock.patch('fileinput.input')
def test_terraform_update_tfvars(mock_fileinput, mock_write, mock_open):
    mock_file = mock.Mock()
    mock_open.return_value = mock_open
    mock_open.__enter__.return_value = mock_file
    mock_fileinput.return_value = ['foo = "baz"', 'env = "dev"']
    variables = {
        'foo': '"bar-%s"'
    }

    TerraformProxy._update_tfvars(variables, 'test.tfvars')

    mock_write.assert_called_once_with('env = "dev"')
    mock_open.assert_called_once_with('test.tfvars', 'a', encoding='utf-8')
    mock_file.write.assert_called_once_with('foo = "bar-baz"\n')


def test_glue_find_job():
    job_executor = GlueJobExecutor(mock.Mock(), mock.Mock(), ['vb-dev-sample-job'])

    actual = job_executor.find_job('sample-job')

    assert actual == 'vb-dev-sample-job'


def test_glue_find_unknown_job():
    job_executor = GlueJobExecutor(mock.Mock(), mock.Mock(), ['vb-dev-sample-job'])

    with pytest.raises(ValueError):
        job_executor.find_job('unknown-job')


def test_reset_job_bookmarks():
    mock_glue = mock.Mock()
    job_executor = GlueJobExecutor(mock_glue, mock.Mock(), ['vb-dev-foo-job', 'vb-dev-bar-job'])

    job_executor.reset_job_bookmark()

    calls = mock_glue.reset_job_bookmark.call_args_list
    assert len(calls) == 2
    assert calls[0][1] == {'JobName': 'vb-dev-foo-job'}
    assert calls[1][1] == {'JobName': 'vb-dev-bar-job'}


def test_reset_job_bookmark():
    mock_glue = mock.Mock()
    job_executor = GlueJobExecutor(mock_glue, mock.Mock(), ['vb-dev-foo-job', 'vb-dev-bar-job'])

    job_executor.reset_job_bookmark('foo-job')

    mock_glue.reset_job_bookmark.assert_called_once_with(JobName='vb-dev-foo-job')


@mock.patch('time.sleep')
def test_glue_submit(mock_sleep):
    mock_glue = mock.Mock()
    mock_glue.start_job_run.return_value = {'JobRunId': 'run#1'}
    mock_glue.get_job_run.side_effect = \
        [{'JobRun': {'JobRunState': o}} for o in ('STARTING', 'RUNNING', 'RUNNING', 'SUCCEEDED')]
    job_executor = GlueJobExecutor(mock_glue, mock.Mock(), ['vb-dev-sample-job'])

    job_executor.submit('sample-job')

    mock_glue.start_job_run.assert_called_once_with(
        JobName='vb-dev-sample-job', Timeout=5, WorkerType='G.1X', NumberOfWorkers=2)
    assert len(mock_glue.get_job_run.call_args_list) == 4
    mock_glue.get_job_run.assert_called_with(JobName='vb-dev-sample-job', RunId='run#1')
    mock_sleep.assert_called_with(20)


@mock.patch('time.sleep')
def test_glue_submit_job_failed(mock_sleep):
    mock_glue = mock.Mock()
    mock_logs = mock.Mock()
    mock_glue.start_job_run.return_value = {'JobRunId': 'run#1'}
    mock_glue.get_job_run.side_effect = \
        [{'JobRun': {'JobRunState': o}} for o in ('STARTING', 'RUNNING', 'FAILED')]
    mock_logs.get_log_events.return_value = {'events': [{'message': 'foo'}]}
    job_executor = GlueJobExecutor(mock_glue, mock_logs, ['vb-dev-sample-job'])

    with pytest.raises(GlueJobExecutionException):
        job_executor.submit('sample-job')

    mock_glue.start_job_run.assert_called_once_with(
        JobName='vb-dev-sample-job', Timeout=5, WorkerType='G.1X', NumberOfWorkers=2)
    assert len(mock_glue.get_job_run.call_args_list) == 3
    mock_glue.get_job_run.assert_called_with(JobName='vb-dev-sample-job', RunId='run#1')
    mock_sleep.assert_called_with(20)
    mock_logs.get_log_events.assert_called_with(
        logGroupName='/aws-glue/jobs/output', logStreamName='run#1', startFromHead=False)


@mock.patch('time.sleep')
def test_glue_submit_job_retry(mock_sleep):
    mock_glue = mock.Mock()
    mock_glue.exceptions.ConcurrentRunsExceededException = ConcurrentRunsExceededException
    mock_glue.start_job_run.side_effect = \
        [ConcurrentRunsExceededException, ConcurrentRunsExceededException, {'JobRunId': 'run#1'}]
    mock_glue.get_job_run.side_effect = \
        [{'JobRun': {'JobRunState': o}} for o in ('STARTING', 'RUNNING', 'RUNNING', 'SUCCEEDED')]
    job_executor = GlueJobExecutor(mock_glue, mock.Mock(), ['vb-dev-sample-job'])

    job_executor.submit('sample-job')

    assert len(mock_glue.start_job_run.call_args_list) == 3
    mock_glue.start_job_run.assert_called_with(
        JobName='vb-dev-sample-job', Timeout=5, WorkerType='G.1X', NumberOfWorkers=2)
    assert len(mock_glue.get_job_run.call_args_list) == 4
    mock_glue.get_job_run.assert_called_with(JobName='vb-dev-sample-job', RunId='run#1')
    mock_sleep.assert_called_with(20)


@mock.patch('time.sleep')
def test_glue_submit_job_not_started(mock_sleep):
    mock_glue = mock.Mock()
    mock_glue.exceptions.ConcurrentRunsExceededException = ConcurrentRunsExceededException
    mock_glue.start_job_run.side_effect = ConcurrentRunsExceededException
    mock_glue.get_job_run.side_effect = \
        [{'JobRun': {'JobRunState': o}} for o in ('STARTING', 'RUNNING', 'RUNNING', 'SUCCEEDED')]
    job_executor = GlueJobExecutor(mock_glue, mock.Mock(), ['vb-dev-sample-job'])

    with pytest.raises(ConcurrentRunsExceededException):
        job_executor.submit('sample-job')

    assert len(mock_glue.start_job_run.call_args_list) == 6
    mock_glue.start_job_run.assert_called_with(
        JobName='vb-dev-sample-job', Timeout=5, WorkerType='G.1X', NumberOfWorkers=2)
    mock_sleep.assert_called_with(20)


class ConcurrentRunsExceededException(Exception):
    """ Internal class used for tests """
