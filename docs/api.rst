.. _api:

Developer Interface
===================

.. module:: bux_sql_grader


Classes
-------
.. autoclass:: bux_sql_grader.mysql.MySQLEvaluator
   :members:

.. autoclass:: bux_sql_grader.mysql.S3UploaderMixin
   :members:

Scoring
-------
.. autoclass:: bux_sql_grader.scoring.MySQLRubricScorer
   :members:
.. autoclass:: bux_sql_grader.scoring.MySQLBaseScorer
   :members:

Exceptions
----------
.. autoexception:: bux_sql_grader.mysql.InvalidQuery
