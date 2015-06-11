all: sdist

sdist:
	python setup.py sdist

clean:
	rm -rf git_bigfile.egg-info
	rm -rf dist
	rm -f gitbigfile/*.pyc

deps vendor_deps: check_setup
	pip install --target=vendor -r requirements.txt

check_setup:
	@command -v pip > /dev/null || echo "missing dependencies: need to install pip"
