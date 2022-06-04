init:
	pip3 install -r ./requirements.txt -r ./requirements-dev.txt

checkstyle:
	pylint ./src/ ./tests/ --rcfile=.pylintrc

test:
	PYTHONPATH="$$PYTHONPATH:./src/" pytest -p no:cacheprovider ./tests/

build:
	python3 setup.py bdist_wheel

clean:
	rm -rf ./.pytest_cache ./build ./dist ./src/glue_pyspark_skeleton.egg-info

.PHONY: init checkstyle test build clean
