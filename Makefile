# Some simple testing tasks (sorry, UNIX only).

PYTHON=venv/bin/python3.4
PSERVE=venv/bin/gunicorn --paste
PIP=venv/bin/pip
FLAKE=venv/bin/flake8
FLAGS=


update:
	$(PYTHON) ./setup.py develop

build:
	python3.4 -m venv venv
	curl -O https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
	venv/bin/python ez_setup.py
	rm -f ./ez_setup.py
	rm -f ./setuptools*.zip
	venv/bin/easy_install pip

	$(PYTHON) ./setup.py develop

dev:
	$(PIP) install flake8 nose coverage
	$(PYTHON) ./setup.py develop

flake:
	$(FLAKE) --exclude=./venv ./


.PHONY: all build update dev flake
