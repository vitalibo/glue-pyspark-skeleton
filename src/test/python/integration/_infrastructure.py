import fileinput
import json
import random
import re
import shutil
import string
import subprocess
import sys
import time
from pathlib import Path

import boto3
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


@pytest.fixture(scope='session', name='infra_props')
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


@pytest.fixture(scope='session', name='boto3_session')
def boto3_session_fixture(infra_props):
    yield boto3.session.Session(
        profile_name=infra_props['profile'],
        region_name=infra_props['region'])


@pytest.fixture(scope='session', name='glue')
def glue_job_executor_fixture(boto3_session, infra_props):
    yield GlueJobExecutor(
        boto3_session.client('glue'),
        boto3_session.client('logs'),
        infra_props['jobs'])


class TerraformProxy:
    """ Class allows to create/destroy infrastructure for integration tests using terraform.
        Internally class delegate calls infrastructure/Makefile. """

    def __init__(self, environment, profile, execution_id):
        self._environment = f'{environment}-{execution_id}'
        self._profile = profile
        self._execution_id = execution_id
        self._workdir = Path(__file__).parents[4] / 'infrastructure'
        self._parent_tfvars = self._workdir / 'vars' / f'{environment}.tfvars'
        self._tfvars = self._workdir / 'vars' / f'{self._environment}.tfvars'

    def apply(self):
        variables = {
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
            'auto_approve': 'true',
            'version': time.time()
        }

        command = f'make {command} --no-print-directory ' + ' '.join([f'{k}={v}' for k, v in options.items()])

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


class GlueJobExecutor:
    """ Provide interface for submit and manage AWS Glue Jobs """

    def __init__(self, glue, logs, jobs):
        self._glue = glue
        self._logs = logs
        self.jobs = jobs
        self._max_attempts = 6
        self._delay = 20

    def submit(self, job_name, attempt=1, **kwargs):
        job_name = self.find_job(job_name)
        params = {
            'Timeout': kwargs.get('Timeout', 5),
            'WorkerType': kwargs.get('WorkerType', 'G.1X'),
            'NumberOfWorkers': kwargs.get('NumberOfWorkers', 2)
        }

        try:
            response = self._glue.start_job_run(JobName=job_name, **params)
            job_run_id = response['JobRunId']
        except self._glue.exceptions.ConcurrentRunsExceededException as e:
            if attempt >= self._max_attempts:
                raise e

            print(f'{job_name} [WAITING]')
            time.sleep(self._delay)
            return self.submit(job_name, attempt=attempt + 1, **kwargs)

        while True:
            response = self._glue.get_job_run(JobName=job_name, RunId=job_run_id)
            state = response['JobRun']['JobRunState']
            print(f'{job_name} [{state}]')
            if state not in ('STARTING', 'RUNNING', 'STOPPING'):
                break

            time.sleep(self._delay)

        if state == 'SUCCEEDED':
            return None

        try:
            logs = self._logs.get_log_events(
                logGroupName='/aws-glue/jobs/output', logStreamName=job_run_id, startFromHead=False)
            for event in logs['events']:
                print(event['message'], end='')
        except self._logs.exceptions.ResourceNotFoundException:
            pass

        error_message = response['JobRun'].get('ErrorMessage', None)
        raise GlueJobExecutionException(error_message if error_message else state)

    def reset_job_bookmark(self, job_name=None):
        jobs = self.jobs
        if job_name:
            jobs = [self.find_job(job_name)]

        for job in jobs:
            try:
                self._glue.reset_job_bookmark(JobName=job)
            except self._glue.exceptions.EntityNotFoundException:
                pass

    def find_job(self, job_name):
        for job in self.jobs:
            if job.endswith(job_name):
                return job
        raise ValueError('Unknown job')


class GlueJobExecutionException(Exception):
    """ Raises when AWS Glue job finished with non-success state """
