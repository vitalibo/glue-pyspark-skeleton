from pyspark.sql import functions as fn

from dp.core.spark import Job, Spark, Source, Sink
from dp.core.util import java

JAVA_METHOD = 'com.github.vitalibo.dataplatform.core.transform.SampleTransform::transform'


class SampleJob(Job):
    """ Sample implementation of Apache Spark job """

    def __init__(
        self,
        people_source: Source,
        department_source: Source,
        wage_sink: Sink,
        threshold: int
    ) -> None:
        self.people_source = people_source
        self.department_source = department_source
        self.wage_sink = wage_sink
        self.threshold = threshold

    def transform(self, spark: Spark) -> None:
        people = spark.extract(self.people_source)
        department = spark.extract(self.department_source)

        df = people \
            .filter(people.age > self.threshold) \
            .join(department, people.dept_id == department.id) \
            .groupBy(department.name, people.gender) \
            .agg(
                fn.avg(people.salary).alias('avg_salary'),
                fn.max(people.age).alias('max_age')) \
            .transform(java.func(JAVA_METHOD))

        spark.load(self.wage_sink, df)
