import logging

from statsd import statsd

log = logging.getLogger(__name__)


class MySQLBaseScorer(object):
    """ Base class for scoring of MySQL query problems.

    :param str student_answer: the student query
    :param tuple student_results: a two item tuple: (result columns,
                                  result rows)
    :param str grader_answer: the student query
    :param tuple grader_results: a two item tuple: (result columns,
                                  result rows)
    :returns: a two item tuple with score (float), message (str)
    :rtype: tuple

    This class defines several test methods that score various aspects of the
    query results.

    Subclasses must implement the ``score`` method to implement a specific
    scoring algorothim.

    """

    KEYWORDS = ["SELECT", "WHERE", "JOIN", "ORDER BY", "ASC", "DESC", "GROUP BY", "LIMIT"]

    DEFAULT_SCALE = {
        "perfect": 1.0,
        "close":  0.8,
        "nicetry": 0.6,
        "decent": 0.4,
        "fail": 0.0
        }

    def __init__(self, student_answer, student_results, grader_answer,
                 grader_results, scale=None):
        self.student_answer = student_answer
        self.student_cols = student_results[0]
        self.student_rows = student_results[1]
        self.grader_answer = grader_answer
        self.grader_cols = grader_results[0]
        self.grader_rows = grader_results[1]

        self.scale = self.parse_scale_map(scale)

        self.missing_keywords = []
        self._tests = []

    def score(self):
        """ Subclasses should implement the scoring algorithm """
        raise NotImplemented

    def parse_scale_map(self, scale_map):
        """ Converts grader payload provided scale to dict """

        if not isinstance(scale_map, dict):
            return self.DEFAULT_SCALE

        # Merge provided scale mapping with defaults
        scale = dict(self.DEFAULT_SCALE.items() + scale_map.items())

        # Cooerce scale values to floats
        for key, val in scale.items():
            try:
                scale[key] = float(val)
            except ValueError:
                # Fallback to default value on fail
                if key in self.DEFAULT_SCALE:
                    scale[key] = self.DEFAULT_SCALE[key]
                else:
                    # Or delete if key is not present in default scale
                    del scale[key]

        return scale

    def test_rows_match(self):
        """ Do result rows match exactly? """
        return self.student_rows == self.grader_rows

    def test_rows_match_unsorted(self):
        """ Do result rows match if sort order is ignored?

        First each row is indiviudally sorted, then the list of rows is sorted
        Can return false passes (incorrectly guess that unsorted data matches)

        TODO: Optimize sorts using numpy
        TODO: Improve accuracy by doing the same sorts on columns of data

        """
        sorted_grader_rows = sorted(sorted(row, reverse=True) for row in self.grader_rows)
        sorted_student_rows = sorted(sorted(row, reverse=True) for row in self.student_rows)

        return (sorted_grader_rows == sorted_student_rows)

    def test_row_counts_match(self):
        """ Do row counts match exactly? """
        return (len(self.student_rows) == len(self.grader_rows))

    def test_row_counts_close(self, threshold=.5):
        """ Are row counts reasonbly close? """
        return (abs(1.0 * len(self.student_rows) - len(self.grader_rows)) /
                len(self.grader_rows)) <= threshold

    def test_cols_match(self):
        """ Do result columns match exactly? """
        return (self.student_cols == self.grader_cols)

    def test_cols_match_unsorted(self):
        """ Do result columns match if sort order is ignored? """
        return (sorted(self.student_cols) == sorted(self.grader_cols))

    def test_col_counts_match(self):
        """ Do column counts match exactly? """
        return (len(self.student_cols) == len(self.grader_cols))

    def test_col_counts_close(self, threshold=.5):
        """ Are column counts reasonably close? """
        return (abs(1.0 * len(self.student_cols) - len(self.grader_cols)) /
                len(self.grader_cols)) <= threshold

    def test_keywords_match(self):
        """ Are SQL keywords in the grader query in the student response?

        Builds a list of missing keywords for use when generating hints.

        """
        # TODO: Use sqlparse to make sure the keywords are placed correctly
        for keyword in self.KEYWORDS:
            if keyword in self.grader_answer.upper():
                if keyword not in self.student_answer.upper():
                    self.missing_keywords.append(keyword)
        return (len(self.missing_keywords) == 0)

    def close(self):
        self.student_cols = None
        self.student_rows = None
        self.grader_cols = None
        self.grader_rows = None

    @property
    def tests(self):
        """ Returns callable test methods (methods prefixed with 'test_') """
        if self._tests:
            return self._tests

        self._tests = []
        attrs = [attr for attr in dir(self) if attr.startswith('test_')]
        for name in attrs:
            attr = getattr(self, name)
            if hasattr(attr, '__call__'):
                self._tests.append(attr)
        return self._tests


class MySQLRubricScorer(MySQLBaseScorer):

    def score(self):
        """ Generate a score using a hard coded rubric approach """
        results = {}

        # TODO: Don't run unneccesary tests, clearer test logic
        for test in self.tests:
            timer = statsd.timer('bux_sql_grader.scoring.%s' % test.__name__).start()
            result = test()
            timer.stop()
            results[test.__name__] = result

        # "Perfect"
        if (results["test_rows_match"] and
           results["test_cols_match"] and
           results["test_keywords_match"]):
            score = self.scale["perfect"]

        # "Really Close":
        # - Columns are either out of order or named incorrectly
        elif (results["test_rows_match_unsorted"] and
              results["test_keywords_match"]):
            score = self.scale["close"]

        # "Nice Try"
        # - Row counts match but not unsorted contents (bad WHERE clause)
        # - Too many / few rows (bad LIMIT)
        elif (((results["test_cols_match_unsorted"] and
                results["test_row_counts_close"]) or
               (results["test_col_counts_match"] and
                results["test_row_counts_match"]) or
               (results["test_cols_match"])) and
              results["test_keywords_match"]):
            score = self.scale["nicetry"]

        # "Decent Attempt"
        # - Row and column counts are in the right ballpark
        elif (results["test_row_counts_close"] and results["test_col_counts_close"]):
            score = self.scale["decent"]

        # "Fail"
        else:
            score = self.scale["fail"]

        # Generate hints based on the test results
        hints = self.generate_hints(results)

        return score, hints

    def generate_hints(self, results):
        """ Examines scoring results, building a list of hints for the student
        depending on which tests passed.

        """
        hints = []

        # Keyword hints
        if not results["test_keywords_match"]:
            hints.append("Missing SQL Keywords: %s" %
                         ", ".join(self.missing_keywords))

        # Row count hints
        if not results["test_row_counts_match"]:
            if len(self.student_rows) > len(self.grader_rows):
                hints.append("Too many rows.")
            else:
                hints.append("Too few rows.")

        # Column count hints
        if not results["test_col_counts_match"]:
            if len(self.student_cols) > len(self.grader_cols):
                hints.append("Too many columns.")
            else:
                hints.append("Too few columns.")

        # Column names / ordering
        elif not results["test_cols_match_unsorted"]:
            hints.append("Columns are named incorrectly.")

        elif not results["test_cols_match"]:
            hints.append("Columns are out of order.")

        elif results["test_row_counts_match"] and not results["test_rows_match_unsorted"]:
            hints.append("Row count and column names are correct. Compare your rows against the expected results.")

        # Row ordering
        elif results["test_rows_match_unsorted"] and not results["test_rows_match"]:
            hints.append("Rows are out of order.")

        return hints
