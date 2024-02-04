from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='datasurface',
    version='0.0.2',
    license='Server Side License V1',
    description='The DSL code for building a datasurface catalog in github',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Billy Newport',
    author_email='billy@billynewport.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.11',
        'Topic :: Database :: Database Engines/Servers',
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=requirements
)
