venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint target_s3_csv -d C,W,unexpected-keyword-arg,duplicate-code

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit --cov target_s3_csv --cov-fail-under=31

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration --cov target_s3_csv --cov-fail-under=71
