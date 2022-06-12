import pytest


@pytest.mark.cases(
    'Case #0: No comments'
)
@pytest.mark.integration
def test_sample_job(case, glue, data_catalog):
    peoples = data_catalog.athena('peoples')
    departments = data_catalog.athena('departments')
    wages = data_catalog.athena('wages')

    peoples.from_resource(
        __file__,
        f'data/sample_job/{case}/people_source.json',
        'data/sample_job/people_schema.json')
    departments.from_resource(
        __file__,
        f'data/sample_job/{case}/department_source.json',
        'data/sample_job/department_schema.json')

    glue.submit('sample-job')

    wages.assert_equals(
        __file__,
        f'data/sample_job/{case}/wage_sink.json',
        'data/sample_job/wage_schema.json',
        order_by=['name', 'gender'])
