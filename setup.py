#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='bux-sql-grader',
    version='0.2.7',
    author='Boston University',
    author_email='webteam@bu.edu',
    url='https://github.com/bu-ist/bux-sql-grader/',
    description='An external grader for evaluation of SQL problems.',
    long_description=open('README.md').read(),
    packages=['bux_sql_grader'],
    scripts=['bin/sqlmon.py'],
    license='LICENSE',
    install_requires=['boto', 'MySQL-python', 'lxml']
)
