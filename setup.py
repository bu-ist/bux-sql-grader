#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='bux-sql-grader',
    version='0.4.1',
    author='Boston University',
    author_email='webteam@bu.edu',
    url='https://github.com/bu-ist/bux-sql-grader/',
    description='An external grader for evaluation of SQL problems.',
    long_description=open('README.md').read(),
    packages=['bux_sql_grader'],
    scripts=['bin/sqlmon.py'],
    license='LICENSE',
    dependency_links=[
        'https://github.com/bu-ist/sql-filter/tarball/0.1.2#egg=bu-sql-filter-0.1.2',
    ],
    install_requires=[
        'boto>=2.38.0, <3.0',
        'MySQL-python>=1.2.5, <1.3',
        'sqlparse>=0.1.15, <0.2',
        'bu-sql-filter==0.1.2',
        ]
)
