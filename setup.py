#!/usr/bin/env python

from __future__ import absolute_import

import io
import os
import sys
import errno
from shutil import rmtree

from setuptools import setup, Command

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'salesforce_bulk',
]

requires = [
    'six',
    'requests>=2.2.1',
    'unicodecsv>=0.14.1',
    'simple-salesforce>=0.69',

]

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.rst' is present in your MANIFEST.in file!
with io.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, 'salesforce_bulk', '__version__.py')) as f:
    exec(f.read(), about)


class PublishCommand(Command):
    """Support setup.py publish."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds...')
            rmtree(os.path.join(here, 'dist'))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        self.status('Building Source and Wheel (universal) distribution...')
        os.system('{0} setup.py sdist bdist_wheel'.format(sys.executable))

        self.status('Uploading the package to PyPi via Twine...')
        os.system('twine upload dist/*')

        sys.exit()

setup(
    name='salesforce-bulk',
    version=about['__version__'],
    description='Python interface to the Salesforce.com Bulk API.',
    long_description=long_description,
    author='Scott Persinger',
    author_email='scottp@heroku.com',
    url='https://github.com/heroku/salesforce-bulk',
    packages=packages,
    package_data={'': ['LICENSE']},
    include_package_data=True,
    install_requires=requires,
    license='MIT',
    zip_safe=False,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ),
    cmdclass={
        'newpublish': PublishCommand,
    },
)
