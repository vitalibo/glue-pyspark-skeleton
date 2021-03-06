from setuptools import setup

setup(
    name='glue-pyspark-skeleton',
    version='1.0.0',
    python_requires='>=3.6',
    packages=[
        'dp',
        'dp.core',
        'dp.core.job',
        'dp.core.util',
        'dp.infra',
        'dp.infra.aws'
    ],
    package_dir={'': 'src/main/python'},
    zip_safe=False,
    install_requires=[],
    platforms='any'
)
