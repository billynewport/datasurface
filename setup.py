"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from setuptools import setup, find_packages


with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='datasurface',
    version='0.0.19',
    license='BSL_v1.1',
    description='Automate the governance, management and movement of data within your enterprise',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Billy Newport',
    author_email='billy@datasurface.com',
    url='https://github.com/billynewport/datasurface',
    classifiers=[
        'License :: Other/Proprietary License',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.12',
        'Topic :: Database :: Database Engines/Servers',
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={"datasurface": ["*.pyi", "**/*.pyi", "py.typed"]},  # Include .pyi files and py.typed file
    install_requires=requirements,
    # Modules with DataPlatforms should register their entry points here
    entry_points={
        'DataPlatforms': []
    }
)
