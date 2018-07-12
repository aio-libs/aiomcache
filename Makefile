# Some simple testing tasks (sorry, UNIX only).

doc:
	cd docs && make html
	echo "open file://`pwd`/docs/_build/html/index.html"

flake:
	python3 -m flake8 aiomcache tests examples

test: flake
	python3 -m pytest tests


cov cover coverage: flake
	python3 -m pytest --cov=aiomcache --cov-report=html --cov-report=term-missing tests
	@echo "open file://`pwd`/htmlcov/index.html"


clean:
	find . -name __pycache__ |xargs rm -rf
	find . -type f -name '*.py[co]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '.*~' -delete
	find . -type f -name '@*' -delete
	find . -type f -name '#*#' -delete
	find . -type f -name '*.orig' -delete
	find . -type f -name '*.rej' -delete
	rm -f .coverage
	rm -rf coverage
	rm -rf docs/_build

.PHONY: all flake test cov clean
