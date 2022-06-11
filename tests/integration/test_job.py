import pytest


@pytest.mark.integration
def test_sample_job(glue):
    glue.submit('sample-job')
