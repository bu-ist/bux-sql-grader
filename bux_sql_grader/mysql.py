"""
    bux_sql_grader.mysql
    ~~~~~~~~~~~~~~~~~~~~

    Defines classes for grading SQL queries and uploading results to S3.

"""

import copy
import csv
import logging
import os

from statsd import statsd

import MySQLdb
import MySQLdb.constants.FIELD_TYPE
import MySQLdb.converters
from MySQLdb import OperationalError, Warning, Error

import sqlfilter
import sqlparse

from string import Template
from StringIO import StringIO

from xml.sax.saxutils import escape as xml_escape

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bux_grader_framework import BaseEvaluator
from bux_grader_framework.exceptions import ImproperlyConfiguredGrader

from .scoring import MySQLRubricScorer


log = logging.getLogger(__file__)

MAX_QUERY_LENGTH = 10000

INVALID_STUDENT_QUERY = Template("""
<div class="error">
    <h4 style="color:#b40">Could not execute query:</h4>
    <pre><code>$error</code></pre>
</div>""")

INVALID_GRADER_QUERY = Template("""
<div class="error">
    <h4 style="color:#b40">Could not execute grader query:</h4>
    <pre><code>$error</code></pre>
    <p>Please report this issue to the course staff.</p>
</div>""")

CORRECT_QUERY = Template("""
<div class="correct">$notices
    <small style="float:right">$download_link</small>
    <h3>Query Results</h3>
    $student_results
</div>""")

INCORRECT_QUERY = Template("""
<div class="error">$notices
    <div style="float:left;width:48%;">
        <small style="float:right">$download_link</small>
        <h3>Your Results</h3>
        $student_results
    </div>
    <div style="float:right; width:48%">
        <h3>Expected Results</h3>
        $grader_results
    </div>
    <div style="clear:both">$hints</div>
</div>""")

DOWNLOAD_ICON_SRC = "data:image/png;base64,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"
DOWNLOAD_LINK = Template("""
<a href="$url"><img src="$icon_src" height="16" width="16" style="vertical-align:sub" /> $message</a>
""")

UPLOAD_FAILED_MESSAGE = """
<small style="color:#b40">Unable to upload results. Please try again later.</small>
"""

WARNING_TMPL = Template("""
<div style="margin-bottom: 15px; padding: 15px 10px 0 10px; border: 1px solid #b40; border-radius: 3px; background-color: #ffd0ca;">
$msg
</div>
""")

EVAL_FAILURE_HINTS = """
<p>It's possible that the query you submitted is returning too large of a result set.</p>
<ul>
<li>Consider adding WHERE clauses to narrow down the result set</li>
<li>Check your JOIN statements and make sure you're joining ON an appropriate column</li>
<li>Prefix your query with EXPLAIN to check for possible inefficiencies</li>
</ul>
"""

SQL_BLACKLIST = (
    "SLEEP",
    "AES_DECRYPT",
    "AES_ENCRYPT",
    "BENCHMARK",
    "DES_DECRYPT",
    "DES_ENCRYPT",
    "ENCRYPT",
    "ExtractValue",
    "FROM_BASE64",
    "GET_LOCK",
    "IS_FREE_LOCK",
    "IS_USED_LOCK",
    "LOAD_FILE",
    "MASTER_POS_WAIT",
    "OLD_PASSWORD",
    "PASSWORD",
    "PROCEDURE_ANALYSE",
    "RANDOM_BYTES",
    "USER",
    "SESSION_USER",
    "SQL_THREAD_WAIT_AFTER_GTIDS",
    "WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS",
    "SYSTEM_USER",
    "UpdateXML",
    "VALIDATE_PASSWORD_STRENGTH",
    "SET"
    )


class InvalidQuery(Exception):
    """ Raised when a SQL query can not be executed """
    pass


class S3UploaderMixin(object):
    """ A mixin that provides a method for uploading contents to S3 """

    DEFAULT_S3_FILENAME = "results.csv"

    def __init__(self, s3_bucket=None, s3_prefix="results",
                 aws_access_key=None, aws_secret_key=None):

        self.s3_bucket = s3_bucket
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.s3_prefix = s3_prefix

    def upload_to_s3(self, contents, path):
        """Upload submission results to S3

        TODO:
            - Use query_auth=False for `generate_url` if bucket is public

        """

        try:
            s3 = S3Connection(self.aws_access_key, self.aws_secret_key)
            bucket = s3.get_bucket(self.s3_bucket, validate=False)

            if self.s3_prefix:
                keyname = os.path.join(self.s3_prefix, path)
            else:
                keyname = path

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

        #: Default grader payload values
        DEFAULT_PAYLOAD = {
            "database": None,
            "answer": None,
            "row_limit": 10,
            "filename": S3UploaderMixin.DEFAULT_S3_FILENAME,
            "upload_results": True,
            "scale": None
        }

        def __init__(self, database, host, user, passwd, port=3306, timeout=10,
                     select_limit=10000, download_icon=None,
                     s3_upload=True, *args, **kwargs):
            self.database = database
            self.host = host
            self.user = user
            self.passwd = passwd
            self.port = int(port)
            self.timeout = timeout
            self.select_limit = select_limit
            self.s3_upload = s3_upload

            # Path to CSV download icon
            if download_icon:
                self.download_icon = download_icon
            else:
                # data-uri fallback
                self.download_icon = DOWNLOAD_ICON_SRC

            super(MySQLEvaluator, self).__init__(*args, **kwargs)

        def db_connect(self, database):
            try:
                db = MySQLdb.connect(self.host, self.user, self.passwd,
                                     database, self.port,
                                     charset='utf8', use_unicode=True,
                                     autocommit=True,
                                     connect_timeout=self.timeout,
                                     conv=self.db_converter())
            except OperationalError as e:
                log.exception("Could not connect to DB")
                raise ImproperlyConfiguredGrader(e)

            return db

        def db_converter(self):
            """ Returns a custom MySQLdb conversions dict that uses str's for everything.

            DECIMAL fields are slow to sort / compare in the scoring methods
            with large row counts.  Converting all fields to str's gives a
            performance improvement, and is safe in this context since we're
            not doing anything with the rows aside from comparing and spitting
            out HTML / CSVs.

            This code is fragile as it's susceptile to changes in new versions
            of MySQL and MySQLdb.

            """
            converter = MySQLdb.converters.conversions.copy()

            converter[MySQLdb.constants.FIELD_TYPE.TINY] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.SHORT] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.LONG] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.FLOAT] = unicode
            # Hack alert! The unicode conversion was resulting in extra
            # precision for DOUBLE columns that was causing issues with
            # R problems. We're keeping them as floats to preserve the
            # current behavior.
            converter[MySQLdb.constants.FIELD_TYPE.DOUBLE] = float
            converter[MySQLdb.constants.FIELD_TYPE.DECIMAL] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.NEWDECIMAL] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.LONGLONG] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.INT24] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.YEAR] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.TIMESTAMP] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.DATETIME] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.TIME] = unicode
            converter[MySQLdb.constants.FIELD_TYPE.DATE] = unicode

            return converter

        def evaluate(self, submission):
            """ Evaluate SQL query problems

            This method is automatically called by the evaluator worker
            process when used with the ``bux_grader_framework``.

            """
            header = submission["xqueue_header"]
            body = submission["xqueue_body"]
            payload = self.parse_grader_payload(body["grader_payload"])
            response = {"correct": False, "score": 0, "msg": ""}

            # Make sure the query length is sane before doing anything with it.
            if not self.is_legal_query_length(body["student_response"]):
                msg = "<p>The SQL grader cannot process queries with over %d characters. Please revise your submission and try again.</p>" % (
                      MAX_QUERY_LENGTH)
                response["msg"] = WARNING_TMPL.substitute(msg=msg)
                return response

            db = self.db_connect(payload["database"])
            self.set_select_limit(db)

            # Evaluate the students response
            student_warnings = []
            student_response = self.filter_query(body["student_response"])
            try:
                student_results = self.execute_query(db, student_response)
            except InvalidQuery as e:
                context = {"error": xml_escape(str(e))}
                response["msg"] = INVALID_STUDENT_QUERY.substitute(context)
                db.close()
                return response

            # Let the student know their query was insane.
            if len(student_results[1]) == self.select_limit:
                student_warnings.append("The result set below is incomplete. Your query was modified to LIMIT results to %d rows. Consider adding a WHERE or LIMIT clause to narrow down results, and check any JOIN statements to make sure you're joining ON the appropriate columns." % self.select_limit)

            # Evaluate the canonical grader answer (if present)
            grader_warnings = []
            grader_response = self.filter_query(payload["answer"])
            if grader_response:
                try:
                    grader_results = self.execute_query(db, grader_response)
                except InvalidQuery as e:
                    context = {"error": xml_escape(str(e))}
                    response["msg"] = INVALID_GRADER_QUERY.substitute(context)
                    db.close()
                    return response

                # Let the course authors know their query was insane.
                if len(grader_results[1]) == self.select_limit:
                    grader_warnings.append("The result set below is incomplete. Your query was modified to LIMIT results to %d rows. Consider adding a WHERE or LIMIT clause to narrow down results, and check any JOIN statements to make sure you're joining ON the appropriate columns." % self.select_limit)

                correct, score, hints = self.grade_results(student_response,
                                                           student_results,
                                                           grader_response,
                                                           grader_results,
                                                           payload["scale"])
            else:
                # If no grader answer was found in the payload this is a
                # sandbox query. These are always correct.
                correct = True
                score = 1.0
                hints = []
                grader_results = None

            # Upload results CSV to S3
            download_link = ""
            if payload["upload_results"]:

                # Ensure student query generated result rows
                if student_results[1]:

                    # Store results by their pull key (hash of pull time + ID)
                    key = header["submission_key"]
                    filename = payload["filename"]

                    # Prefix filename if student response was incorrect
                    if not correct:
                        filename = "incorrect-" + filename
                    filepath = os.path.join(key, filename)

                    download_link = self.upload_results(student_results,
                                                        filepath)

            # Build the grader response dict
            response = self.build_response(correct=correct,
                                           score=score,
                                           hints=hints,
                                           student_results=student_results,
                                           student_warnings=student_warnings,
                                           grader_results=grader_results,
                                           grader_warnings=grader_warnings,
                                           row_limit=payload["row_limit"],
                                           download_link=download_link)

            db.close()
            return response

        def is_legal_query_length(self, query):
            """ Checks a query to determine if it exceeds MAX_QUERY_LENGTH. """
            if len(query) > MAX_QUERY_LENGTH:
                log.warn("Query length exceeds MAX_QUERY_LENGTH: %d",
                         len(query))
                return False
            return True

        def set_select_limit(self, db):
            """ Set the SQL_SELECT_LIMIT for this session.

            For protection against poorly conceived queries.

            """
            if not self.select_limit:
                return

            cursor = db.cursor()
            cursor.execute("SET SQL_SELECT_LIMIT = %d" % self.select_limit)
            cursor.close()

        def filter_query(self, query):
            """ Filter SQL query to remove any blacklisted keywords """
            if not query:
                return query

            # Make sure LIMIT clauses are sane
            query = self.enforce_select_limit(query)

            # Remove terms from blacklist
            filtered = sqlfilter.filter_sql(query, SQL_BLACKLIST, False)
            if filtered != query:
                log.warning("SQL query was filtered. Before: %s After: %s", query, filtered)

            return filtered

        def enforce_select_limit(self, query):
            """ Examines queries to ensure LIMIT clauses do not exceed our select_limit. """
            if not self.select_limit:
                return query

            enforced = False

            stmts = sqlparse.parse(query)
            for stmt in stmts:
                limit = stmt.token_next_match(0, sqlparse.tokens.Keyword, 'LIMIT')
                while limit:
                    value = stmt.token_next(stmt.token_index(limit))

                    # LIMIT may be an identifier list (e.g. LIMIT 5,10).
                    # Attempt to extract second value from the group.
                    if isinstance(value, sqlparse.sql.Token) and value.is_group():
                        value = value.token_next(1)

                    # Ensure LIMIT is a legal value (integer) before modifying.
                    if isinstance(value, sqlparse.sql.Token) and value.ttype == sqlparse.tokens.Number.Integer:
                        limit_val = int(value.value)
                        if limit_val > self.select_limit:
                            value.value = unicode(self.select_limit)
                            enforced = True

                    # LIMIT is ... something else.  Probably invalid SQL. Log and ignore it.
                    else:
                        log.warning("Unexpected value following LIMIT clause: %s  Query: %s", unicode(value), unicode(stmt))

                    # Get next LIMIT statement
                    limit = stmt.token_next_match(stmt.token_index(limit) + 1, sqlparse.tokens.Keyword, 'LIMIT')

            # Reconstruct the query from the modified sqlparse objects
            if enforced:
                query = "".join([unicode(stmt) for stmt in stmts])
                log.warning("Enforced SQL_SELECT_LIMIT for query: %s (original limit = %d)", query, limit_val)

            return query

        def execute_query(self, db, stmt):
            """ Execute the SQL query

                :param db: a MySQLdb connection object
                :param string stmt: the SQL query to run

                :raises InvalidQuery: if the query could not be executed

            """
            timer = statsd.timer('bux_sql_grader.execute_query').start()
            cursor = db.cursor()
            try:
                cursor.execute(stmt)
                rows = cursor.fetchall()

                cols = ()
                if cursor.description:
                    # Cursor descriptions are not returned as unicode by
                    # MySQLdb so we convert them to support unicode chars in
                    # column headings.
                    cols = tuple(unicode(col[0], 'utf-8') for col in cursor.description)

                cursor.close()
            except (OperationalError, Warning, Error) as e:
                msg = e.args[1]
                code = e.args[0]
                raise InvalidQuery("MySQL Error {}: {}".format(code, msg))
            finally:
                timer.stop()
            return cols, rows

        def grade_results(self, student_answer, student_results, grader_answer,
                          grader_results, scale=None):
            """ Compares student and grader responses to generate a score """

            # Generate a score
            timer = statsd.timer('bux_sql_grader.grade_results').start()
            scorer = MySQLRubricScorer(student_answer, student_results,
                                       grader_answer, grader_results, scale)
            score, messages = scorer.score()
            scorer.close()
            correct = (score == 1)
            timer.stop()
            return correct, score, messages

        def upload_results(self, results, path, message=None):
            """ Upload query results CSV to Amazon S3

                :param tuple results: query results for upload
                :param str path: bucket path
                :param str message: text to display before download link
                :return: link text on successful upload, failure message if
                         s3 upload failed

            """
            timer = statsd.timer('bux_sql_grader.upload_results').start()
            if not message:
                message = "Download full results"

            # Convert result rows to CSV
            csv_results = self.csv_results(results)

            # Upload to S3
            s3_url = self.upload_to_s3(csv_results, path)
            if s3_url:
                context = {"url": xml_escape(s3_url), "message": xml_escape(message),
                           "icon_src": xml_escape(self.download_icon)}
                download_link = DOWNLOAD_LINK.substitute(context)
            else:
                download_link = UPLOAD_FAILED_MESSAGE

            timer.stop()
            return download_link

        def build_response(self, correct, score, hints, student_results,
                           student_warnings=[], grader_results=None,
                           grader_warnings=[], row_limit=None,
                           download_link=""):
            """ Builds a grader response dict. """

            response = {"correct": correct, "score": score}

            # Response message template context
            context = {"download_link": download_link, "hints": ""}

            # Generate student response results table
            context["student_results"] = self.html_results(student_results,
                                                           row_limit)

            # Generate warning messages if queries had to be modified
            notices = ""
            if student_warnings:
                student_warnings = [xml_escape(notice) for notice in student_warnings]
                student_warning = "<strong>Warning</strong><p>"
                student_warning += "</p><p>".join(student_warnings) + "</p>"
                notices += WARNING_TMPL.substitute(msg=student_warning)

            if grader_warnings:
                grader_warnings = [xml_escape(notice) for notice in grader_warnings]
                grader_warning = "<strong>Warning</strong><p>"
                grader_warning += "</p><p>".join(grader_warnings) + "</p>"
                notices += WARNING_TMPL.substitute(msg=grader_warning)

            context["notices"] = notices

            if grader_results and not correct:

                # Generate grader response results table
                context["grader_results"] = self.html_results(grader_results,
                                                              row_limit)

                # Generate hints markup if hints were provided
                if hints:
                    # Ensure hint text is XML-safe
                    hints = [xml_escape(hint) for hint in hints]

                    hints_html = "<strong>Hints</strong>"
                    hints_html += "<ul><li>"
                    hints_html += "</li><li>".join(hints)
                    hints_html += "</li></ul>"

                    context["hints"] = hints_html

                # Incorrect response template
                response["msg"] = INCORRECT_QUERY.substitute(context)
            else:
                # Correct response template
                response["msg"] = CORRECT_QUERY.substitute(context)

            return response

        def csv_results(self, results):
            """ Format result set for display as CSV """
            cols, rows = results

            sio = StringIO()
            writer = csv.writer(sio)

            if cols:
                writer.writerow([self.format_csv_col(s) for s in cols])

            for row in rows:
                writer.writerow([self.format_csv_col(s) for s in row])

            csv_results = sio.getvalue()
            sio.close()
            return csv_results

        def format_html_col(self, col):
            """ Format a result column value for HTML """
            formatted = xml_escape(unicode(col))

            if col is None:
                formatted = "NULL"

            return formatted

        def format_csv_col(self, col):
            """ Format a result column value for CSV """
            formatted = unicode(col).encode('utf-8')

            if col is None:
                formatted = ""

            return formatted

        def html_results(self, results, row_limit=None):
            """ Format result set for display as HTML """
            cols, rows = results
            row_count = len(rows)

            if row_count < 1:
                return "<pre><code>No rows found.</code></pre>"

            html = "<pre><code><table><thead>"
            html += u"<tr><th>{}</th></tr>".format(u"</th><th>".join(self.format_html_col(col) for col in cols))
            html += "</thead><tbody>"

            for idx, row in enumerate(rows):
                if row_limit and idx >= row_limit:
                    break
                html += u"<tr><td>{}</td></tr>".format(
                        u"</td><td>".join(self.format_html_col(col) for col in row))

            html += "</tbody></table></code></pre>"

            # Stats
            if row_limit and row_count > row_limit:
                html += self.result_stats(row_limit, row_count)
            else:
                html += self.result_stats(row_count, row_count)

            return html

        def result_stats(self, displayed, total):
            return "<p><small>Showing %d of %s row%s.</small></p>" % (displayed, total,
                                                                      "s"[total == 1:])

        def parse_grader_payload(self, payload):
            """ Parses the grader payload JSON object.

            Missing keys will be filled in from the ``DEFAULT_PAYLOAD`` dict.

            """
            defaults = copy.deepcopy(self.DEFAULT_PAYLOAD)

            # Update class defaults with instance values
            defaults["database"] = self.database
            defaults["upload_results"] = self.s3_upload

            # Merge defaults with passed in values
            payload = dict(defaults.items() + payload.items())

            # Payload sanitization
            payload["row_limit"] = self.sanitize_row_limit(payload["row_limit"])

            return payload

        def sanitize_row_limit(self, limit):
            """ Cleans the ``row_limit`` value passed in the grader payload """
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                limit = None

            if limit < 1:
                limit = None

            return limit

        def fail_hints(self):
            """ Hints to be appended to evaluation failure messages.

            This method is called by the grader framework when an evaluation
            fails. We're offering tips on query optimization in case a bad
            query was the root cause.

            """
            return EVAL_FAILURE_HINTS

        def status(self):
            """ Assert that a DB connection can be made """
            try:
                db = self.db_connect(self.database)
            except ImproperlyConfiguredGrader:
                return False
            else:
                db.close()
                return True
