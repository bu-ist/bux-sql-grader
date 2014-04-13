"""
    bux_sql_grader.mysql
    ~~~~~~~~~~~~~~~~~~~~

    Defines classes for grading SQL queries and uploading results to S3.

"""

import csv
import logging

import MySQLdb
from MySQLdb import OperationalError, Warning, Error

from string import Template
from StringIO import StringIO

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bux_grader_framework import BaseEvaluator
from bux_grader_framework.exceptions import ImproperlyConfiguredGrader


log = logging.getLogger(__file__)

INVALID_STUDENT_QUERY = Template("""
<div class="error">
    <p>Could not execute query: <code>$query</code></p>
    <h4>Error:</h4>
    <pre><code>$error</code></pre>
</div>""")

INVALID_GRADER_QUERY = Template("""
<div class="error">
    <p><strong>Invalid grader query</strong>: <code>$query</code></p>
    <p>Please report this issue to the course staff.</p>
    <h4>Error:</h4>
    <pre><code>$error</code></pre>
</div>""")

CORRECT_QUERY = Template("""
<div class="correct">
    <h3>Query results:</h3>
    $results
</div>""")

INCORRECT_QUERY = Template("""
<div class="error">
    <h3>Expected Results</h3>
    $expected
    <h3>Your Results</h3>
    $actual
</div>""")

DOWNLOAD_MESSAGE = Template("""
<p>Download your full results as CSV: <a href="$url">$filename</a></p>
""")

UPLOAD_FAILED_MESSAGE = """
<p>Could not upload results file. Please contact course staff.</p>
"""


class InvalidQuery(Exception):
    """ Raised when a SQL query can not be executed """
    pass


class S3UploaderMixin(object):
    """ A mixin that provides a method for uploading contents to S3 """

    DEFAULT_S3_FILENAME = "results.csv"

    def __init__(self, s3_bucket, s3_prefix, aws_access_key, aws_secret_key):

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key

    def upload_to_s3(self, contents, path, name=None):
        """Upload submission results to S3

        TODO:
            - Use query_auth=False for `generate_url` if bucket is public

        """
        name = name or self.DEFAULT_S3_FILENAME

        try:
            s3 = S3Connection(self.aws_access_key, self.aws_secret_key)
            bucket = s3.get_bucket(self.s3_bucket, validate=False)

            keyname = "{prefix}/{path}/{name}".format(prefix=self.s3_prefix,
                                                      path=path, name=name)
            key = Key(bucket, keyname)
            key.set_contents_from_string(contents, replace=True)
            s3_url = key.generate_url(60*60*24)
        except Exception as e:
            log.error("Error uploading results to S3: %s", e)
            s3_url = False

        return s3_url


class MySQLEvaluator(S3UploaderMixin, BaseEvaluator):
        """ An evaluator class that handles SQL problems. """

        name = "mysql"

        def __init__(self, database, host, user, passwd, port=3306, timeout=10,
                     *args, **kwargs):
            self.database = database
            self.host = host
            self.user = user
            self.passwd = passwd
            self.port = port
            self.timeout = timeout

            super(MySQLEvaluator, self).__init__(*args, **kwargs)

        def db_connect(self, database):
            try:
                db = MySQLdb.connect(self.host, self.user, self.passwd,
                                     database, self.port,
                                     connect_timeout=self.timeout)
            except OperationalError as e:
                log.exception("Could not connect to DB")
                raise ImproperlyConfiguredGrader(e)

            return db

        def evaluate(self, submission):
            """ Evaluate SQL query problems

            This method is automatically called by the evaluator worker
            process when used with the ``bux_grader_framework``.

            """
            header = submission["xqueue_header"]
            body = submission["xqueue_body"]
            payload = body["grader_payload"]

            database = payload.get("database", self.database)

            row_limit = payload.get("row_limit", None)
            row_limit = self.sanitize_row_limit(row_limit)

            upload_results = payload.get("upload_results", True)

            db = self.db_connect(database)

            response = {"correct": False, "score": 0, "msg": ""}

            # Evaluate the students response
            student_response = body["student_response"]
            try:
                stu_results = self.execute_query(db, student_response)
            except InvalidQuery as e:
                context = {"query": student_response, "error": e}
                response["msg"] = INVALID_STUDENT_QUERY.substitute(context)
                return response

            # Evaluate the canonical grader answer (if present)
            grader_answer = payload.get("answer")
            if grader_answer:
                try:
                    grader_results = self.execute_query(db, grader_answer)
                except InvalidQuery as e:
                    context = {"query": grader_answer, "error": e}
                    response["msg"] = INVALID_GRADER_QUERY.substitute(context)
                    return response

                correct, score = self.grade_results(stu_results,
                                                    grader_results)
                response = self.build_response(correct, score,
                                               stu_results, grader_results,
                                               row_limit)
            else:
                # If no grader answer was found in the payload this is a
                # sandbox query. These are always correct.
                response = self.build_response(True, 1, stu_results,
                                               row_limit)

            # Upload student results to S3 if student query returned any rows
            # Append the download link to the response message on success.
            if upload_results and stu_results[1]:
                key = header["submission_key"]
                filename = payload.get("filename", self.DEFAULT_S3_FILENAME)
                download_link = self.upload_results(stu_results, key, filename)
                if download_link:
                    response["msg"] += download_link

            # Ensure the message is LMS-ready
            response["msg"] = self.sanitize_message(response["msg"])

            return response

        def execute_query(self, db, stmt):
            """ Execute the SQL query

                :param db: a MySQLdb connection object
                :param string stmt: the SQL query to run

                :raises InvalidQuery: if the query could not be executed

            """
            cursor = db.cursor()
            try:
                cursor.execute(stmt)
                rows = cursor.fetchall()

                cols = []
                if cursor.description:
                    cols = [str(col[0]) for col in cursor.description]
            except (OperationalError, Warning, Error) as e:
                msg = e.args[1]
                code = e.args[0]
                raise InvalidQuery("MySQL Error {}: {}".format(code, msg))
            return cols, rows

        def grade_results(self, stu_results, grader_results):
            """ Compares student and grader responses to generate a score """
            stu_cols, stu_rows = stu_results
            ans_cols, ans_rows = grader_results

            if stu_rows == ans_rows:
                return True, 1
            return False, 0

        def upload_results(self, results, path, filename):
            """ Upload student results CSV """
            csv_results = self.csv_results(results)

            s3_url = self.upload_to_s3(csv_results, path, filename)
            if s3_url:
                context = {"url": s3_url, "filename": filename}
                download_link = DOWNLOAD_MESSAGE.substitute(context)
            else:
                download_link = UPLOAD_FAILED_MESSAGE

            return download_link

        def build_response(self, correct, score, student_results,
                           grader_results=None, row_limit=None):
            """ Build a message """

            response = {"correct": correct, "score": score}

            student_html = self.html_results(student_results, row_limit)

            if grader_results and not correct:
                grader_html = self.html_results(grader_results, row_limit)
                context = {"expected": grader_html, "actual": student_html}
                message = INCORRECT_QUERY.substitute(context)
            else:
                context = {"results": student_html}
                message = CORRECT_QUERY.substitute(context)

            response["msg"] = message
            return response

        def csv_results(self, results):
            """ Format result set for display as CSV """
            cols, rows = results

            sio = StringIO()
            writer = csv.writer(sio)

            if cols:
                writer.writerow(cols)

            for row in rows:
                writer.writerow(row)

            csv_results = sio.getvalue()
            sio.close()
            return csv_results

        def html_results(self, results, row_limit=None):
            """ Format result set for display as HTML """
            cols, rows = results
            row_count = len(rows)

            if row_count < 1:
                return "<pre><code>No rows found.</code></pre>"

            html = "<pre><code><table><thead>"
            html += "<tr><th>{}</th></tr>".format("</th><th>".join(cols))
            html += "</thead><tbody>"

            for idx, row in enumerate(rows):
                if row_limit and idx >= row_limit:
                    break
                html += "<tr><td>{}</td></tr>".format(
                        "</td><td>".join(str(col) for col in row))

            html += "</tbody></table></code></pre>"

            # Stats
            if row_limit and row_count > row_limit:
                html += self.result_stats(row_limit, row_count)
            else:
                html += self.result_stats(row_count, row_count)

            return html

        def result_stats(self, displayed, total):
            return "<p>Showing %d of %s row%s.</p>" % (displayed, total,
                                                       "s"[total == 1:])

        def sanitize_row_limit(self, limit):
            """ Cleans the ``row_limit`` value passed in the grader payload """
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                limit = None

            if limit < 1:
                limit = None

            return limit

        def sanitize_message(self, message):
            """ Ensure that the message does not contain invalid XML entities.

            The LMS runs grader messages through lxml.etree.fromstring which
            fails with invalid XML, resulting in:

                "Invalid grader reply. Please contact the course staff."

           """
            message = message.replace("&", "&amp;")

            # The LMS is very strict about message formatting. We wrap
            # the message in a div so that there is one root element.
            # The additional markup added by the download link will break
            # things otherwise.
            message = "<div>" + message + "</div>"

            return message
