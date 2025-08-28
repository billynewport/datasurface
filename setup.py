"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from setuptools import setup, find_packages
import platform


with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    all_requirements = f.read().splitlines()

# Filter out problematic packages on ARM64
requirements = []
for req in all_requirements:
    if req.strip() and not req.strip().startswith('#'):
        # Skip IBM DB packages on ARM64 (aarch64)
        if platform.machine() == 'aarch64' and ('ibm_db' in req or 'ibm-db-sa' in req):
            continue
        requirements.append(req)

setup(
    name='datasurface',
    version='0.0.53',
    license='BSL_v1.1',
    description='Automate the governance, management and movement of data within your enterprise',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Billy Newport',
    author_email='billy@datasurface.com',
    url='https://github.com/billynewport/datasurface',
    python_requires='>=3.13',
    classifiers=[
        'License :: Other/Proprietary License',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database :: Database Engines/Servers',
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={"datasurface": ["py.typed", "*.pyi", "**/*.pyi"]},  # Include py.typed file and type stubs
    zip_safe=False,  # Required for py.typed to work properly
    install_requires=requirements,
    # Modules with DataPlatforms should register their entry points here
    entry_points={
        'DataPlatforms': []
    }
)
