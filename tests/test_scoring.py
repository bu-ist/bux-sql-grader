import json
import unittest

from . import TESTS_DIR

from bux_sql_grader.scoring import MySQLWeightedScorer, rows_match


class TestMySQLWeightedScorer(unittest.TestCase):

    def test_score(self):
        with open(TESTS_DIR + "/scoring_fixtures/scores.json") as f:
            fixtures = json.load(f)

        for fixture in fixtures:
            scorer = MySQLWeightedScorer(fixture["student_answer"],
                                         fixture["student_results"],
                                         fixture["grader_answer"],
                                         fixture["grader_results"],
                                         fixture["payload"]
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
        payload = {"score_map": json.dumps({"foo": 0.5})}
        scorer = MySQLWeightedScorer(None, None, None, None, payload)

        self.assertEquals(0.5, scorer.score_map['foo'])
        for key in MySQLWeightedScorer.DEFAULT_SCORE_MAP:
            self.assertEquals(MySQLWeightedScorer.DEFAULT_SCORE_MAP[key],
                              scorer.score_map[key])

    def test_score_map_overrides(self):
        payload = {"score_map": json.dumps({"rows_match": 0.5})}
        scorer = MySQLWeightedScorer(None, None, None, None, payload)
        self.assertEquals(0.5, scorer.score_map["rows_match"])

    def test_score_map_invalid_reverts_to_default(self):
        payload = {"score_map": "foo"}
        scorer = MySQLWeightedScorer(None, None, None, None, payload)

        self.assertEquals(MySQLWeightedScorer.DEFAULT_SCORE_MAP,
                          scorer.score_map)
