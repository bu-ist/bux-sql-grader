import json
import unittest

from . import TESTS_DIR

from bux_sql_grader.scoring import MySQLScorer, rows_match


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

    def test_rows_match_correct(self):
        r1 = (('a', 'b'), ('c', 'd'))
        r2 = (('a', 'b'), ('c', 'd'))
        self.assertEquals(1, rows_match(r1, r2))

    def test_rows_match_incorrect(self):
        r1 = (('a', 'b'), ('c', 'd'))
        r2 = (('a', 'b'), ('e', 'f'))
        self.assertEquals(0, rows_match(r1, r2))

    def test_score_map(self):
        extra_map = {"foo": 0.5}
        scorer = MySQLScorer(None, None, None, None, extra_map)

        self.assertEquals(0.5, scorer.score_map['foo'])
        for key in MySQLScorer.DEFAULT_SCORE_MAP:
            self.assertEquals(MySQLScorer.DEFAULT_SCORE_MAP[key],
                              scorer.score_map[key])

    def test_score_map_overrides(self):
        extra_map = {"rows_match": 0.5}
        scorer = MySQLScorer(None, None, None, None, extra_map)
        self.assertEquals(0.5, scorer.score_map["rows_match"])

    def test_score_map_raises_value_error(self):
        extra_map = "foo"
        self.assertRaises(ValueError, MySQLScorer, None, None, None, None,
                          extra_map)
