import json
import unittest

from . import TESTS_DIR

from bux_sql_grader.scoring import MySQLRubricScorer


class TestMySQLRubricScorer(unittest.TestCase):

    def test_score(self):
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

    # Row tests

    def test_rows_match(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_rows_match())

    def test_rows_match_incorrect(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('d', 'c')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_rows_match())

    def test_rows_match_unsorted(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('c', 'd'), ('a', 'b')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_rows_match_unsorted())

    def test_rows_match_unsorted_incorrect(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'c'), ('d', 'c')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_rows_match_unsorted())

    def test_row_counts_match(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_row_counts_match())

    def test_row_counts_match_incorrect(self):
        stu_results = (('col1', 'col2'), (('a', 'b'), ('c', 'd')))
        grader_results = (('col1', 'col2'), (('a', 'b'),))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_row_counts_match())

    def test_row_counts_close(self):
        stu_results = (('col1',), (('a',), ('b',)))
        grader_results = (('col1',), (('a',), ('b',), ('c',), ('d',)))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_row_counts_close())

    def test_row_counts_close_incorrect(self):
        stu_results = (('col1',), (('a',),))
        grader_results = (('col1',), (('a',), ('b',), ('c',), ('d',)))
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_row_counts_close())

    # Column tests

    def test_cols_match(self):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_cols_match())

    def test_cols_match_incorrect(self):
        stu_results = (('col2', 'col1'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_cols_match())

    def test_cols_match_unsorted(self):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col2', 'col1'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_cols_match_unsorted())

    def test_cols_match_unsorted_incorrect(self):
        stu_results = (('col1', 'col3'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_cols_match_unsorted())

    def test_col_counts_match(self):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_col_counts_match())

    def test_col_counts_match_incorrect(self):
        stu_results = (('col1',), ())
        grader_results = (('col1', 'col2'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_col_counts_match())

    def test_col_counts_close(self):
        stu_results = (('col1', 'col2'), ())
        grader_results = (('col1', 'col2', 'col3', 'col4'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertTrue(scorer.test_col_counts_close())

    def test_col_counts_close_incorrect(self):
        stu_results = (('col1',), ())
        grader_results = (('col1', 'col2', 'col3', 'col4'), ())
        scorer = MySQLRubricScorer('', stu_results, '', grader_results)
        self.assertFalse(scorer.test_col_counts_close())
