import json
import unittest

from mock import patch

from . import TESTS_DIR

from bux_sql_grader.scoring import MySQLRubricScorer


@patch('bux_sql_grader.scoring.statsd')
class TestMySQLRubricScorer(unittest.TestCase):

    def test_score(self, mock_statsd):
        with open(TESTS_DIR + "/scoring_fixtures/scores.json") as f:
            fixtures = json.load(f)

        for fixture in fixtures:
            scorer = MySQLRubricScorer(fixture["student_answer"],
                                       fixture["student_results"],
                                       fixture["grader_answer"],
                                       fixture["grader_results"],
                                       )
            self.assertEquals(tuple(fixture["expected_score"]),
                              scorer.score())

    def test_score_custom_scale(self, mock_statsd):
        with open(TESTS_DIR + "/scoring_fixtures/scores_custom_scale.json") as f:
            fixtures = json.load(f)

        for fixture in fixtures:
            scorer = MySQLRubricScorer(fixture["student_answer"],
                                       fixture["student_results"],
                                       fixture["grader_answer"],
                                       fixture["grader_results"],
                                       fixture["scale"]
                                       )
            self.assertEquals(tuple(fixture["expected_score"]),
                              scorer.score())

    # Row tests

    def test_rows_match(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_rows_match())

    def test_rows_match_incorrect(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('d', 'c')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_rows_match())

    def test_rows_match_unsorted(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('c', 'd'), ('a', 'b')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_rows_match_unsorted())

    def test_rows_match_unsorted_incorrect(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'c'), ('d', 'c')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_rows_match_unsorted())

    def test_row_counts_match(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_row_counts_match())

    def test_row_counts_match_incorrect(self, mock_statsd):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'),))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_row_counts_match())

    def test_row_counts_close(self, mock_statsd):
        stu_results = (('col1',), (('a',), ('b',)))
        grader_results = (('col1',), (('a',), ('b',), ('c',), ('d',)))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_row_counts_close())

    def test_row_counts_close_incorrect(self, mock_statsd):
        stu_results = (('col1',), (('a',),))
        grader_results = (('col1',), (('a',), ('b',), ('c',), ('d',)))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_row_counts_close())

    # Column tests

    def test_cols_match(self, mock_statsd):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_cols_match())

    def test_cols_match_incorrect(self, mock_statsd):
        stu_results = (('col2', 'col1'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_cols_match())

    def test_cols_match_unsorted(self, mock_statsd):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col2', 'col1'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_cols_match_unsorted())

    def test_cols_match_unsorted_incorrect(self, mock_statsd):
        stu_results = (('col1', 'col3'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_cols_match_unsorted())

    def test_col_counts_match(self, mock_statsd):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_col_counts_match())

    def test_col_counts_match_incorrect(self, mock_statsd):
        stu_results = (('col1',), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_col_counts_match())

    def test_col_counts_close(self, mock_statsd):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2', 'col3', 'col4'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_col_counts_close())

    def test_col_counts_close_incorrect(self, mock_statsd):
        stu_results = (('col1',), ())
        grader_results = (('col1', 'col2', 'col3', 'col4'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_col_counts_close())

    def test_parse_scale_map(self, mock_statsd):
        scale_map = {"perfect": 0.9, "close": 0.8, "nicetry": 0.7, "decent": 0.6, "fail": 0.5}
        scorer = MySQLRubricScorer('', ((), ()), '', ((), ()))
        self.assertEquals(scale_map, scorer.parse_scale_map(scale_map))

    def test_parse_scale_map_defaults(self, mock_statsd):
        scale_map = None
        scorer = MySQLRubricScorer('', ((), ()), '', ((), ()))
        self.assertEquals(scorer.DEFAULT_SCALE, scorer.parse_scale_map(scale_map))

    def test_parse_scale_map_float_values(self, mock_statsd):
        scale_map = {"perfect": 5}
        scorer = MySQLRubricScorer('', ((), ()), '', ((), ()))
        scale_map = scorer.parse_scale_map(scale_map)
        self.assertTrue(isinstance(scale_map["perfect"], float))

    def test_parse_scale_map_skips_invalid_values(self, mock_statsd):
        scale_map = {"perfect": "foo"}
        scorer = MySQLRubricScorer('', ((), ()), '', ((), ()))
        scale_map = scorer.parse_scale_map(scale_map)
        self.assertEquals(scorer.DEFAULT_SCALE["perfect"], scale_map["perfect"])

    def test_parse_scale_map_removes_invalid_keys(self, mock_statsd):
        scale_map = {"foo": "bar"}
        scorer = MySQLRubricScorer('', ((), ()), '', ((), ()))
        scale_map = scorer.parse_scale_map(scale_map)
        self.assertNotIn("foo", scale_map)
