# pipelinewise-target-s3-csv

[![PyPI version](https://badge.fury.io/py/pipelinewise-target-s3-csv.svg)](https://badge.fury.io/py/pipelinewise-target-s3-csv)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-target-s3-csv.svg)](https://pypi.org/project/pipelinewise-target-s3-csv/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that uploads loads data to S3 in CSV format
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this target is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Target S3 CSV](https://transferwise.github.io/pipelinewise/connectors/targets/s3_csv.html)

If you want to run this [Singer Target](https://singer.io) independently please read further.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-target-s3-csv
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-s3-csv --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Postgres.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Configuration settings

Running the the target connector requires a `config.json` file. An example with the minimal settings:

   ```json
   {
     "aws_access_key_id": "secret",
     "aws_secret_access_key": "secret",
     "s3_bucket": "my_bucket"
   }
   ```

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| aws_access_key_id                   | String  | Yes        | S3 Access Key Id                                              |
| aws_secret_access_key               | String  | Yes        | S3 Secret Access Key                                          |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can 
| delimiter                           | String  |            | (Default: ',') A one-character string used to separate fields. |
| quotechar                           | String  |            | (Default: '"') A one-character string used to quote fields containing special characters, such as the delimiter or quotechar, or which contain new-line characters. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |
| encryption_type                     | String  | No         | (Default: 'none') The type of encryption to use. Current supported options are: 'none' and 'KMS'. |
| encryption_key                      | String  | No         | A reference to the encryption key to use for data encryption. For KMS encryption, this should be the name of the KMS encryption key ID (e.g. '1234abcd-1234-1234-1234-1234abcd1234'). This field is ignored if 'encryption_type' is none or blank. |
| compression                         | String  | No         | The type of compression to apply before uploading. Supported options are `none` (default) and `gzip`. For gzipped files, the file extension will automatically be changed to `.csv.gz` for all files. |

### To run tests:

1. Define environment variables that requires running the tests
```
  export TARGET_S3_CSV_ACCESS_KEY_ID=<s3-access-key-id>
  export TARGET_S3_CSV_SECRET_ACCESS_KEY=<s3-secret-access-key>
  export TARGET_S3_CSV_BUCKET=<s3-bucket>
  export TARGET_S3_CSV_KEY_PREFIX=<s3-key-prefix>
```

2. Install python test dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .[test]
```

3. To run unit tests:
```
  nosetests --where=tests/unit
```

4. To run integration tests:
```
  nosetests --where=tests/integration
```

### To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_s3_csv -d C,W,unexpected-keyword-arg,duplicate-code
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.

