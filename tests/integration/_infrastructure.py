import fileinput
import json
import random
import re
import shutil
import string
import subprocess
import sys
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption('--environment', action='store')
    parser.addoption('--profile', action='store', default='default')
    parser.addoption('--execution_id', action='store',
                     default=''.join(random.choices(string.ascii_lowercase + string.digits, k=5)))


def pytest_configure(config):
    config.addinivalue_line('markers', 'integration: mark a test as a integration test.')


def pytest_collection_modifyitems(config, items):
    integration_mark = 'integration' in config.getoption('-m')
    if integration_mark:
        return

    remaining = []
    deselected = []
    for item in items:
        if 'integration' not in item.keywords:
            remaining.append(item)
        else:
            deselected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = remaining


@pytest.fixture(scope='session', name='terraform')
def terraform_fixture(pytestconfig):
    config = {k: pytestconfig.getoption(k) for k in ('environment', 'profile', 'execution_id')}
    if config['environment'] is None:
        raise ValueError('following argument is required: --environment')

    terraform = TerraformProxy(**config)
    try:
        output = terraform.apply()
        yield output
    finally:
        terraform.destroy()


class TerraformProxy:
    """ Class allows to create/destroy infrastructure for integration tests using terraform.
        Internally class delegate calls infrastructure/Makefile. """

    def __init__(self, environment, profile, execution_id):
        self._environment = f'{environment}-{execution_id}'
        self._profile = profile
        self._execution_id = execution_id
        self._workdir = Path(__file__).parents[2] / 'infrastructure'
        self._parent_tfvars = self._workdir / 'vars' / f'{environment}.tfvars'
        self._tfvars = self._workdir / 'vars' / f'{self._environment}.tfvars'

    def apply(self):
        variables = {
            'environment': '"%s-test"',
            'name': f'"{self._execution_id}-%s"'
        }

        shutil.copyfile(self._parent_tfvars, self._tfvars)
        self._update_tfvars(variables, self._tfvars)
        self._execute('apply')

        out, _ = self._execute('output', stdout=subprocess.PIPE)
        return {k: v['value'] for k, v in json.loads(out).items()}

    def destroy(self):
        self._execute('destroy')
        self._tfvars.unlink()
        shutil.rmtree(self._workdir / '.terraform' / self._environment)

    def _execute(self, command, **kwargs):
        options = {
            'environment': self._environment,
            'profile': self._profile,
            'auto_approve': 'true'
        }

        command = f'make {command} ' + ' '.join([f'{k}={v}' for k, v in options.items()])

        with subprocess.Popen(args=command.split(), cwd=self._workdir, **kwargs) as process:
            our, err = process.communicate()
            if process.returncode != 0:
                raise subprocess.SubprocessError(f'terraform {command} finished with errors')
            return our, err

    @staticmethod
    def _update_tfvars(variables, path):
        for line in fileinput.input(path, inplace=True):
            remain = True
            for key, value in variables.items():
                matcher = re.match(f'^{key} += (.*)', line)
                if matcher:
                    variables[key] = value.replace('%s', matcher.group(1)[1:-1])
                    remain = False
                    break
            if remain:
                sys.stdout.write(line)

        with open(path, 'a', encoding='utf-8') as file:
            for key, value in variables.items():
                file.write(f'{key} = {value}\n')
