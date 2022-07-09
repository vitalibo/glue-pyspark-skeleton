init:
	pip3 install -r ./requirements.txt -r ./requirements-dev.txt

checkstyle:
	pylint ./src/main/python/* ./src/test/python/* --rcfile=.pylintrc
	mvn pmd:check

test:
	PYTHONPATH="$$PYTHONPATH:./src/main/python/" pytest -p no:cacheprovider ./src/test/python/
	mvn test

integration_test:
	PYTHONPATH="$$PYTHONPATH:./src/main/python/" pytest -p no:cacheprovider -s -m integration ./src/test/python/ \
		--environment=$(environment) --profile=$(profile)

build:
	python3 setup.py bdist_wheel
	mvn clean package -Dmaven.test.skip

clean:
	rm -rf ./.pytest_cache ./build ./dist ./src/main/python/glue_pyspark_skeleton.egg-info
	mvn clean

.PHONY: init checkstyle test integration_test build clean
