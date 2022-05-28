init:
	pip3 install -r ./requirements.txt -r ./requirements-dev.txt

test:
	PYTHONPATH="$$PYTHONPATH:./src/" pytest -p no:cacheprovider ./tests/

clean:
	rm -rf ./.pytest_cache

.PHONY: init
