from setuptools import setup, find_packages

#Distribute py wheels
#python3 setup.py bdist_wheel sdist
#twine check dist/*
#cd dist 
#twine upload *

with open("README.md", "r") as fh:
    long_description = fh.read()


setup (
	name="mongodata",
	version="0.0.3",
	description="Abstraction over common db interactions",
	url="https://github.com/licenseware/lware-components-mongodata",
	author="licenseware",
	author_email="contact@licenseware.io",
	license='',
	py_modules=["mongodata"],
	install_requires=["pymongo", "marshmallow"],
	packages=find_packages(exclude=("tests",)),
	long_description=long_description,
    long_description_content_type="text/markdown",
	package_dir={"":"src"}
)