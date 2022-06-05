import subprocess
from unittest import mock

import pytest

from integration._infrastructure import TerraformProxy


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

    actual = terraform._execute('apply')

    assert actual == ('output', 'error')
    mock_subprocess.communicate.assert_called_once()
    mock_subprocess.__enter__.assert_called_once()
    args = mock_popen.call_args
    assert args[1]['args'] == 'make apply environment=dev-1q2w3e profile=my auto_approve=true'.split(' ')
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
