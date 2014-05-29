import copy
import unittest
import MySQLdb

from MySQLdb.cursors import Cursor

from mock import MagicMock, patch

from bux_grader_framework.exceptions import ImproperlyConfiguredGrader
from bux_sql_grader.mysql import MySQLEvaluator, InvalidQuery, INVALID_STUDENT_QUERY, INVALID_GRADER_QUERY

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "passwd": "root",
    "database": "foo",
    "port": 3306,
    "timeout": 10
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

DUMMY_SUBMISSION = {
    "xqueue_header": {
        "submission_id": 123,
        "submission_key": "abc"
    },
    "xqueue_body": {
        "student_response": "SELECT * FROM foo",
        "student_info": {
            "anonymous_student_id": "abc",
            "submission_time": "2014"
        },
        "grader_payload": {
            "database": "foo",
            "answer": "SELECT * FROM foo",
            "filename": "foo.csv",
            "row_limit": 10,
            "upload_results": True,
            "scale": None
        }
    }
}


@patch('bux_sql_grader.mysql.statsd', autospec=True)
@patch('bux_sql_grader.mysql.MySQLdb', autospec=True)
class TestMySQLEvaluator(unittest.TestCase):

    def setUp(self):
        CONFIG = dict(MYSQL_CONFIG.items() + S3_CONFIG.items())
        self.grader = MySQLEvaluator(**CONFIG)

    def test_db_connect(self, mock_db, mock_statsd):
        self.grader.db_connect('foo')

        mock_db.connect.assert_called_with(MYSQL_CONFIG["host"],
                                           MYSQL_CONFIG["user"],
                                           MYSQL_CONFIG["passwd"],
                                           'foo',
                                           MYSQL_CONFIG["port"],
                                           connect_timeout=10,
                                           charset='utf8',
                                           use_unicode=True,
                                           autocommit=True,
                                           )

    def test_db_connect_raises_exception(self, mock_db, mock_statsd):
        mock_db.connect.side_effect = MySQLdb.OperationalError

        self.assertRaises(ImproperlyConfiguredGrader,
                          self.grader.db_connect, 'bar')

    def test_execute_query(self, mock_db, mock_statsd):
        mock_cursor = MagicMock(spec=Cursor)
        mock_cursor.fetchall.return_value = DUMMY_QUERY['rows']
        mock_cursor.description = DUMMY_QUERY['description']

        db = MagicMock()
        db.cursor = MagicMock(return_value=mock_cursor)

        results = self.grader.execute_query(db, DUMMY_QUERY['query'])
        self.assertEquals(DUMMY_QUERY['result'], results)

    def test_execute_query_invalid(self, mock_db, mock_statsd):
        mock_cursor = MagicMock(spec=Cursor)
        mock_cursor.execute.side_effect = MySQLdb.Error(1146, 'Table does not exist')

        db = MagicMock()
        db.cursor = MagicMock(return_value=mock_cursor)

        self.assertRaises(InvalidQuery,
                          self.grader.execute_query, db, DUMMY_QUERY['query'])

    def test_evaluate(self, mock_db, mock_statsd):
        results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        download_link = '<p>Download link: <a href="#">foo.csv</a></p>'
        row_limit = DUMMY_SUBMISSION["xqueue_body"]["grader_payload"]["row_limit"]

        # Mock query results and S3 upload
        self.grader.execute_query = MagicMock(return_value=results)
        self.grader.upload_results = MagicMock(return_value=download_link)

        # Build response using grader API
        expected = self.grader.build_response(True, 1.0, [], results, results, row_limit, download_link)

        self.assertEquals(expected, self.grader.evaluate(DUMMY_SUBMISSION))

    def test_evaluate_invalid_student_query(self, mock_db, mock_statsd):
        query = DUMMY_SUBMISSION["xqueue_body"]["student_response"]
        error_msg = "Bad student query"

        # Raises exception on first call (student query)
        self.grader.execute_query = MagicMock(side_effect=InvalidQuery(error_msg))

        expected = {
            "correct": False,
            "score": 0,
            "msg": INVALID_STUDENT_QUERY.substitute(query=query, error=error_msg)
        }
        self.assertEquals(expected, self.grader.evaluate(DUMMY_SUBMISSION))

    def test_evaluate_invalid_grader_query(self, mock_db, mock_statsd):
        query = DUMMY_SUBMISSION["xqueue_body"]["grader_payload"]["answer"]
        error_msg = "Bad grader query"

        # Needs to succeed on first call (student query)
        def side_effect(*args):
            def second_call(*args):
                raise InvalidQuery(error_msg)
            mock_exec_query.side_effect = second_call
            return

        mock_exec_query = MagicMock(side_effect=side_effect)
        self.grader.execute_query = mock_exec_query

        expected = {
            "correct": False,
            "score": 0,
            "msg": INVALID_GRADER_QUERY.substitute(query=query, error=error_msg)
        }
        self.assertEquals(expected, self.grader.evaluate(DUMMY_SUBMISSION))

    def test_evaluate_row_limit(self, mock_db, mock_statsd):
        results = (('col1',), (('a',), ('b',), ('c'), ('d',), ('e',)))
        submission = copy.deepcopy(DUMMY_SUBMISSION)
        submission["xqueue_body"]["grader_payload"]["row_limit"] = 3

        # Mock query results and S3 upload
        self.grader.execute_query = MagicMock(return_value=results)
        self.grader.upload_results = MagicMock(return_value='')

        response = self.grader.evaluate(submission)

        self.assertIn("Showing 3 of 5 rows", response["msg"])

    def test_evaluate_row_limit_no_limit(self, mock_db, mock_statsd):
        results = (('col1',), (('a',), ('b',), ('c'), ('d',), ('e',)))
        submission = copy.deepcopy(DUMMY_SUBMISSION)

        # Mock query results and S3 upload
        self.grader.execute_query = MagicMock(return_value=results)
        self.grader.upload_results = MagicMock(return_value='')

        for limit in [0, False, None]:
            submission["xqueue_body"]["grader_payload"]["row_limit"] = limit
            response = self.grader.evaluate(submission)
            self.assertIn("Showing 5 of 5 rows", response["msg"])

    def test_evaluate_sandbox_query(self, mock_db, mock_statsd):
        results = (('col1',), (('a',), ('b',), ('c'), ('d',), ('e',)))
        submission = copy.deepcopy(DUMMY_SUBMISSION)
        submission["xqueue_body"]["grader_payload"]["answer"] = None

        # Mock query results and S3 upload
        self.grader.execute_query = MagicMock(return_value=results)
        self.grader.upload_results = MagicMock(return_value='')

        expected = self.grader.build_response(True, 1.0, [], results)
        actual = self.grader.evaluate(submission)

        self.assertEquals(expected, actual)

    def test_evaluate_sandbox_query_respects_row_limit(self, mock_db, mock_statsd):
        results = (('col1',), (('a',), ('b',), ('c'), ('d',), ('e',)))
        submission = copy.deepcopy(DUMMY_SUBMISSION)
        submission["xqueue_body"]["grader_payload"]["answer"] = None
        submission["xqueue_body"]["grader_payload"]["row_limit"] = 2

        # Mock query results and S3 upload
        self.grader.execute_query = MagicMock(return_value=results)
        self.grader.upload_results = MagicMock(return_value='')

        response = self.grader.evaluate(submission)

        self.assertIn("Showing 2 of 5 rows", response["msg"])

    def test_upload_results(self, mock_db, mock_statsd):
        pass

    def test_build_response(self, mock_db, mock_statsd):
        pass

    def test_parse_grader_payload(self, mock_db, mock_statsd):
        payload = DUMMY_SUBMISSION['xqueue_body']['grader_payload']

        self.assertEquals(payload, self.grader.parse_grader_payload(payload))

    def test_parse_grader_payload_defaults(self, mock_db, mock_statsd):
        payload = {}
        default = copy.deepcopy(self.grader.DEFAULT_PAYLOAD)

        # Will update default payload to include instance database
        self.grader.database = default["database"] = "bar"

        parsed = self.grader.parse_grader_payload(payload)
        self.assertEquals(default, parsed)

    def test_parse_grader_payload_overrides_defaults(self, mock_db, mock_statsd):
        payload = DUMMY_SUBMISSION['xqueue_body']['grader_payload']
        payload["database"] = "baz"
        payload["filename"] = "bar.csv"

        self.assertEquals(payload, self.grader.parse_grader_payload(payload))

    def test_parse_grader_payload_sanitizes_row_limit(self, mock_db, mock_statsd):
        payload = DUMMY_SUBMISSION['xqueue_body']['grader_payload']
        payload["row_limit"] = "foo"

        parsed = self.grader.parse_grader_payload(payload)
        self.assertEquals(None, parsed["row_limit"])

    def test_sanitize_row_limit(self, mock_db, mock_statsd):
        pass

    def test_sanitize_message(self, mock_db, mock_statsd):
        pass

    def test_csv_results(self, mock_db, mock_statsd):
        pass

    def test_html_results(self, mock_db, mock_statsd):
        pass
