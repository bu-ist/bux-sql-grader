import unittest
import MySQLdb

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor

from mock import MagicMock, patch

from bux_grader_framework.exceptions import ImproperlyConfiguredGrader
from bux_sql_grader.mysql import MySQLEvaluator

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "passwd": "root",
    "database": "foo",
    "port": 3306,
}

S3_CONFIG = {
    "s3_bucket": "foo",
    "s3_prefix": "results",
    "aws_access_key": "abc",
    "aws_secret_key": "123",
}

DUMMY_QUERY = {
    "query": "SELECT yearID, HR FROM Batting WHERE yearID = '2010' ORDER BY HR DESC LIMIT 10",
    "description" : (('yearID', 3, 4, 11, 11, 0, 0), ('HR', 3, 2, 11, 11, 0, 1)),
    "rows" : ((2010L, 54L), (2010L, 42L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L)),
    "result": (['yearID', 'HR'], ((2010L, 54L), (2010L, 42L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L)))
}

CORRECT_RESULT = {
    "args": ("SELECT yearID, HR FROM Batting WHERE yearID = '2010' ORDER BY HR DESC LIMIT 10", (['yearID', 'HR'], ((2010L, 54L), (2010L, 42L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L))), "SELECT yearID, HR FROM Batting WHERE yearID = '2010' ORDER BY HR DESC LIMIT 10", (['yearID', 'HR'], ((2010L, 54L), (2010L, 42L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L))), {}),
    "score": (True, 1.0, [])
}

INCORRECT_RESULT = {
    "args": ("SELECT yearID, HR FROM Batting WHERE yearID = '2010' ORDER BY HR ASC LIMIT 10", (['yearID', 'HR'], ((2010L, 42L), (2010L, 54L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L))), "SELECT yearID, HR FROM Batting WHERE yearID = '2010' ORDER BY HR DESC LIMIT 10", (['yearID', 'HR'], ((2010L, 54L), (2010L, 42L), (2010L, 39L), (2010L, 38L), (2010L, 38L), (2010L, 37L), (2010L, 34L), (2010L, 33L), (2010L, 33L), (2010L, 32L))), {}),
    "score": (False, 0, [])
}


@patch('bux_sql_grader.mysql.S3UploaderMixin', autospec=True)
class TestMySQLEvaluator(unittest.TestCase):

    def setUp(self):
        CONFIG = dict(MYSQL_CONFIG.items() + S3_CONFIG.items())
        self.grader = MySQLEvaluator(**CONFIG)

    @patch('bux_sql_grader.mysql.MySQLdb', autospec=True)
    def test_db_connect(self, mock_db, mock_s3):
        self.grader.db_connect('foo')

        mock_db.connect.assert_called_with(MYSQL_CONFIG["host"],
                                           MYSQL_CONFIG["user"],
                                           MYSQL_CONFIG["passwd"],
                                           'foo',
                                           MYSQL_CONFIG["port"],
                                           )

    @patch('bux_sql_grader.mysql.MySQLdb', autospec=True)
    def test_db_connect_raises_exception(self, mock_db, mock_s3):
        mock_db.connect.side_effect = MySQLdb.OperationalError

        self.assertRaises(ImproperlyConfiguredGrader,
                          self.grader.db_connect, 'bar')

    def test_execute_query(self, mock_s3):
        mock_cursor = MagicMock(spec=Cursor)
        mock_cursor.fetchall.return_value = DUMMY_QUERY['rows']
        mock_cursor.description = DUMMY_QUERY['description']

        mock_db = MagicMock(spec=Connection)
        mock_db.cursor.return_value = mock_cursor

        results = self.grader.execute_query(mock_db, DUMMY_QUERY['query'])
        self.assertEquals(DUMMY_QUERY['result'], results)

    def test_evaluate(self, mock_s3):
        pass

    def test_grade_results(self, mock_s3):
        score = self.grader.grade_results(*CORRECT_RESULT['args'])
        self.assertEquals(CORRECT_RESULT['score'], score)

    def test_grade_results_incorrect(self, mock_s3):
        score = self.grader.grade_results(*INCORRECT_RESULT['args'])
        self.assertEquals(INCORRECT_RESULT['score'], score)

    def test_upload_results(self, mock_s3):
        pass

    def test_build_response(self, mock_s3):
        pass

    def test_sanitize_row_limit(self, mock_s3):
        pass

    def test_sanitize_message(self, mock_s3):
        pass

    def test_csv_results(self, mock_s3):
        pass

    def test_html_results(self, mock_s3):
        pass
