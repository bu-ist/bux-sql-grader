import logging
import random

from bux_grader_test_framework import GraderTestRunner


class SQLGraderRunner(GraderTestRunner):

    QUERIES = [
        "SELECT yearID, HR FROM Batting WHERE yearID = 2010 ORDER BY HR DESC LIMIT 10",
        "SELECT HR, yearID FROM Batting LIMIT 5",
    ]

    def get_submission(self):
        answer = random.choice(self.QUERIES)
        if random.choice([True, False, False]):
            student_response = answer
        else:
            student_response = random.choice(self.QUERIES)

        grader_payload = {
            "answer": answer,
            "evaluator": "mysql",
            "upload_results": False
        }
        return student_response, grader_payload


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    sql_tests = SQLGraderRunner('load_tests.example_settings', count=100)
    sql_tests.run()
