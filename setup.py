#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-s3-csv",
      version="1.4.0",
      description="Singer.io target for writing CSV files and upload to S3 - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-target-s3-csv',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_s3_csv"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'inflection==0.3.1',
          'boto3==1.17.33',
      ],
      extras_require={
          "test": [
              "nose==1.3.7",
              "pylint==2.4.2"
          ]
      },
      entry_points="""
          [console_scripts]
          target-s3-csv=target_s3_csv:main
       """,
      packages=["target_s3_csv"],
      package_data = {},
      include_package_data=True,
)
