import argparse
import importlib
from itertools import accumulate

import yaml
from pyspark import SparkConf, SparkContext

from dp.core.spark import Spark, Job, Source, Sink


class Factory:
    """ Factory class for all objects """

    def __init__(self, argv):
        full_conf = self._parse_conf(file='application.yaml')
        self._job_name = self._parse_job_name(argv, full_conf)
        self._conf = full_conf[self._job_name]
        self._argv = argv

    def create_spark(self) -> Spark:
        """ Create PySpark runner """

        conf = SparkConf()
        conf.set('spark.sql.session.timeZone', 'UTC')
        properties = self._conf.get('properties', {})
        for key, value in properties.items():
            conf.set(key, value)

        sc = SparkContext(conf=conf)

        from dp.infra.aws.glue import GlueSpark  # pylint: disable=import-outside-toplevel
        return GlueSpark(sc)

    def create_job(self) -> Job:
        """ Create instance of Job """

        kwargs = self._parse_kwargs(self._conf)
        module = importlib.import_module(f'dp.core.job.{self._job_name}')
        job = getattr(module, self._job_name.title().replace('_', ''))
        return job(**kwargs)

    def _create_source(self, name, conf) -> Source:  # noqa pylint: disable=no-self-use
        if 'connection_type' in conf and conf['connection_type'] == 's3':
            from dp.infra.aws import glue  # pylint: disable=import-outside-toplevel
            return glue.GlueSource(**{'transformation_ctx': name, **conf})
        if 'java_class' in conf:
            from dp.core.util import java  # pylint: disable=import-outside-toplevel
            return java.JavaSource(conf['java_class'], *conf['args'])

        raise ValueError(f'Unsupported source: {name}')

    def _create_sink(self, name, conf) -> Sink:  # noqa pylint: disable=no-self-use
        if 'connection_type' in conf and conf['connection_type'] == 's3':
            from dp.infra.aws import glue  # pylint: disable=import-outside-toplevel
            return glue.GlueSink(**{'transformation_ctx': name, **conf})
        if 'java_class' in conf:
            from dp.core.util import java  # pylint: disable=import-outside-toplevel
            return java.JavaSink(conf['java_class'], *conf['args'])

        raise ValueError(f'Unsupported sink: {name}')

    def _create_decorator(self, name, conf):  # pylint: disable=unused-argument
        kwargs = self._parse_kwargs(conf)
        *module, cls = conf['class'].split('.')
        module = importlib.import_module('.'.join(module))
        decorator = getattr(module, cls)
        return decorator(**kwargs)

    def _parse_kwargs(self, super_conf):
        kwargs = {}
        for name, conf in super_conf['kwargs'].items():
            name, *method_name = name.split('@')
            if len(method_name) > 0:
                method = getattr(self, f'_create_{method_name[0]}')
                kwargs[name] = method(name, conf)
            else:
                kwargs[name] = conf

        return kwargs

    @staticmethod
    def _parse_job_name(argv, conf) -> str:
        job_name = Factory._parse_argv(argv, 'JOB_NAME')
        variants = accumulate(job_name.split('-')[::-1], lambda *s: '_'.join(s[::-1]))
        for name in variants:
            if name in conf:
                return name

        raise ValueError('Not recognized')

    @staticmethod
    def _parse_argv(argv, option) -> str:
        parser = _ArgumentParser()
        parser.add_argument('--' + option, required=True)
        parsed, _ = parser.parse_known_args(argv[1:])
        return vars(parsed)[option.replace('-', '_')]

    @staticmethod
    def _parse_conf(file) -> dict:
        def merge(src, dst):
            for key, value in src.items():
                if isinstance(value, dict):
                    node = dst.setdefault(key, {})
                    merge(value, node)
                else:
                    dst[key] = value
            return dst

        with open(file, 'r', encoding='utf-8') as f:
            conf = {}
            for doc in yaml.safe_load_all(f):
                conf = merge(doc, conf)

        return conf


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)
