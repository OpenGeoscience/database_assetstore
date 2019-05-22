#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json

# Based on https://github.com/pypa/sampleproject
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from os import path
# io.open is needed for projects that support Python 2.7
# It ensures open() defaults to text mode with universal newlines,
# and accepts an argument to specify the text encoding
from io import open

ownDir = path.abspath(path.dirname(__file__))

with open(path.join(ownDir, 'README.rst'), encoding='utf-8') as f:
    readme = f.read()

with open(path.join(ownDir, 'plugin.json'), encoding='utf-8') as f:
    pkginfo = json.load(f)

with open(path.join(ownDir, 'LICENSE'), encoding='utf-8') as f:
    license_str = f.read()

extras_require = {
    'mysql': ['mysqlclient>=1.3.10'],
    'postgres': ['psycopg2>=2.7.1'],
    'sqlite': [],
}
all_extras = set()
for key in extras_require:
    all_extras.update(extras_require[key])
extras_require['all'] = list(all_extras)

setup(
    name='girder-database-assetstore',
    version=pkginfo['version'],
    description=pkginfo['description'],
    long_description=readme,
    long_description_content_type='text/x-rst; charset=UTF-8',
    url='https://github.com/OpenGeoscience/database_assetstore',
    maintainer='Kitware, Inc.',
    maintainer_email='kitware@kitware.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='girder database assetstore',
    packages=find_packages(exclude=['plugin_tests']),
    install_requires=['sqlalchemy>=1.1.11'],
    extras_require=extras_require,
    data_files=[
        ('database_assetstore/girder', ['plugin.json']),
    ],
    test_suite='plugin_tests',
    entry_points={
        'girder.plugin': [
            'database_assetstore = database_assetstore:DatabaseAssetstorePlugin'
        ]
    }
)
