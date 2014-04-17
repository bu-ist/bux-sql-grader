import logging
import random

from bux_grader_test_framework import GraderTestRunner


class SQLGraderRunner(GraderTestRunner):

    QUERIES = [
        "SELECT yearID, HR FROM Batting WHERE yearID = 2010 ORDER BY HR DESC LIMIT 10",
        "SELECT playerID, HR FROM Batting WHERE yearID = 2011 ORDER BY HR DESC LIMIT 5",
        "SELECT HR, yearID FROM Batting LIMIT 5",
        "SELECT * FROM Batting WHERE teamID = 'SEA'",
        "SELECT * FROM Batting WHERE teamID = 'BOS' AND yearID = 2013",
        "SELECT * FROM Batting WHERE teamID = 'BOS' AND yearID = 2013 AND AB > 0",
        "SELECT *, H/AB AS BA FROM Batting WHERE yearID = 2013 AND teamID = 'BOS' AND AB > 0",
        "SELECT *, H/AB AS BA, (H+2B+2*3B+3*HR)/AB AS SLG FROM Batting WHERE yearID = 2013 AND teamID = 'BOS' AND AB > 0",
        "SELECT playerID, G, AB+BB+HBP+SF AS PA, AB, H, 2B, 3B, HR, R, RBI, SB, H/AB AS BA, (H+BB+HBP)/(AB+BB+HBP+SF) AS OBP, (H+2B+2*3B+3*HR)/AB AS SLG FROM Batting WHERE yearID = '2013' AND teamID = 'BOS' AND AB IS NOT NULL HAVING PA >= 1 ORDER BY PA DESC"
    ]

    def get_submission(self):
        """ Generate a student submission for a SQL grader problem """
        # Simulation uses random queries / correctness
        if self.simulation:
            answer = random.choice(self.QUERIES)
            if random.choice([True, False, False]):
                student_response = answer
            else:
                student_response = random.choice(self.QUERIES)
        else:
            # Non-simulation just picks a single typical query and sticks to it (for repeatability)
            answer = self.QUERIES[0]
            student_response = self.QUERIES[1]

        grader_payload = {
            "answer": answer,
            "evaluator": "mysql",
            "upload_results": True
        }
        return student_response, grader_payload


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    sql_tests = SQLGraderRunner('load_tests.example_settings',
                                submissions_per_second=10,
                                count=50,
                                simulation=False)
    sql_tests.run()
