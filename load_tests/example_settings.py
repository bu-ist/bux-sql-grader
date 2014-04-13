
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_HOST = "127.0.0.1"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "/"

EVALUATOR_MODULES = {
    "bux_sql_grader"
}

EVALUATOR_CONFIG = {
    "mysql": {
        "database": "",
        "host": "127.0.0.1",
        "user": "",
        "passwd": "",
        "port": 3306,
        "aws_access_key": "",
        "aws_secret_key": "",
        "s3_bucket": "",
        "s3_prefix": "",
    }
}

DEFAULT_EVALUATOR = "mysql"
WORKER_COUNT = 2
