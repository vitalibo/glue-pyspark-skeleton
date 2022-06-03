import pytest

from dp.core.job.sample_job import SampleJob


@pytest.mark.cases(
    'Case #0: age is less than 20',
    'Case #1: age equals to 20',
    'Case #2: age is more than 20',
    'Case #3: no dept id for joining',
    'Case #4: group by department',
    'Case #5: group by gender',
    'Case #6: average salary',
    'Case #7: maximum age',
    'Case #8: empty dataframes'
)
def test_sample_job(case, spark):
    job = SampleJob(
        spark.create_source_from_resource(
            __file__,
            f'data/sample_job/{case}/people_source.json',
            'data/sample_job/people_schema.json'),
        spark.create_source_from_resource(
            __file__,
            f'data/sample_job/{case}/department_source.json',
            'data/sample_job/department_schema.json'),
        spark.create_sink_from_resource(
            __file__,
            f'data/sample_job/{case}/wage_sink.json',
            'data/sample_job/wage_schema.json'),
        20)

    spark.submit(job)
