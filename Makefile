init:
	pip3 install -r ./requirements.txt -r ./requirements-dev.txt

checkstyle:
	pylint ./src/ ./tests/ --rcfile=.pylintrc

test:
	PYTHONPATH="$$PYTHONPATH:./src/" pytest -p no:cacheprovider ./tests/

clean:
	rm -rf ./.pytest_cache

.PHONY: init checkstyle test
