#!/usr/bin/env python

from setuptools import setup

setup(name="pipelinewise-target-s3-csv",
      version="1.0.0",
      description="Singer.io target for writing CSV files and upload to S3 - PipelineWise compatible",
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-target-s3-csv',
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["target_s3_csv"],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python==2.1.4',
          'inflection==0.3.1',
          'boto3==1.9.57',
          'backoff==1.3.2'
      ],
      entry_points="""
          [console_scripts]
          target-s3-csv=target_s3_csv:main
       """,
      packages=["target_s3_csv"],
      package_data = {},
      include_package_data=True,
)
