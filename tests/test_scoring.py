import json
import unittest

from . import TESTS_DIR

from bux_sql_grader.scoring import MySQLScorer


class TestMySQLScorer(unittest.TestCase):

    def test_score(self):
        with open(TESTS_DIR + "/scoring_fixtures/scores.json") as f:
            fixtures = json.load(f)

        for fixture in fixtures:
            scorer = MySQLScorer(fixture["student_answer"],
                                 fixture["student_results"],
                                 fixture["grader_answer"],
                                 fixture["grader_results"],
                                 )
            self.assertEquals(tuple(fixture["expected_score"]),
                              scorer.score())

    def test_rows_match(self):
        pass

    def test_keywords_match(self):
        with open(TESTS_DIR + "/scoring_fixtures/keyword_matches.json") as f:
            fixtures = json.load(f)

        for fixture in fixtures:
            scorer = MySQLScorer(fixture["student_answer"],
                                 fixture["student_results"],
                                 fixture["grader_answer"],
                                 fixture["grader_results"],
                                 )
            self.assertEquals(tuple(fixture["expected_score"]),
                              scorer.keywords_match())
