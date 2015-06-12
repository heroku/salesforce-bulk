#!/usr/bin/env python

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'salesforce_bulk',
]

requires = [
    'httplib2>=0.7.5',
    'requests>=2.2.1',
]

with open('README.md') as f:
    readme = f.read()
with open('LICENSE') as f:
    license = f.read()

setup(
    name='salesforce-bulk',
    version='1.0.9',
    description='Python interface to the Salesforce.com Bulk API.',
    long_description=readme,
    author='Scott Persinger',
    author_email='scottp@heroku.com',
    url='https://github.com/heroku/salesforce-bulk',
    packages=packages,
    package_data={'': ['LICENSE']},
    include_package_data=True,
    install_requires=requires,
    license=license,
    zip_safe=False,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
)
