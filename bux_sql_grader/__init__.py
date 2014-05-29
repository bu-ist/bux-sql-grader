"""
    bux_sql_grader
    ~~~~~~~~~~~~~~

    An evaluator module for use grading SQL queries with the edX platform.

    Designed to be used with the `BU edX Grader Framework
    <https://github.com/bu-ist/bux-grader-framework>`_

    :copyright: 2014 Boston University
    :license: GNU Affero General Public License
"""

__version__ = '0.2.4'

from .mysql import MySQLEvaluator
