.DEFAULT_GOAL := build
.PHONY: build publish package package-test docs venv


build:
	pip install --editable .

freeze:
	pip freeze > requirements.txt

lint:
	pylint sf_bulk_api_ws

coverage: test
	mkdir -p docs/build/html
	coverage html

docs: coverage
	cd docs && $(MAKE) html

package: clean docs
	python setup.py sdist

package-test:
	python setup.py sdist

publish: package
	twine upload dist/* --repository nexus

clean :
	rm -rf dist \
	rm -rf docs/build \
	rm -rf *.egg-info
	coverage erase

test: lint
	py.test --cov . tests/

venv :
	virtualenv --python python3.6 venv

install:
	pip install -r requirements.txt

docker-build:
	docker build -t sf_bulk_api_ws .

docker-start: docker-build
	docker run -e KAFKA_SERVERS=server_1:9200,server_2,server_3 -e KAFKA_TASK_TOPIC=my-task-topic -e KAFKA_LOGIC_TOPIC=my-logic-topic -d sf_bulk_api_ws
