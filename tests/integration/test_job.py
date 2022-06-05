import re

import pytest


@pytest.mark.integration
def test_sample_job(terraform):
    assert re.match('dev-test-[a-z0-9]+-gps-sample-job', terraform['sample_job_name'])
