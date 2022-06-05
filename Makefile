init:
	pip3 install -r ./requirements.txt -r ./requirements-dev.txt

checkstyle:
	pylint ./src/ ./tests/ --rcfile=.pylintrc

test:
	PYTHONPATH="$$PYTHONPATH:./src/" pytest -p no:cacheprovider ./tests/

integration_test:
	PYTHONPATH="$$PYTHONPATH:./src/" pytest -p no:cacheprovider -s -m integration ./tests/ \
		--environment=$(environment) --profile=$(profile)

build:
	python3 setup.py bdist_wheel

clean:
	rm -rf ./.pytest_cache ./build ./dist ./src/glue_pyspark_skeleton.egg-info

.PHONY: init checkstyle test integration_test build clean
