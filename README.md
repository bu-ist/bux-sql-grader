# edX SQL Grader

An [external grader](http://ca.readthedocs.org/en/latest/problems_tools/external_graders.html) for evaluation of SQL problems.

Designed for use with the [edX External Grader framework](https://github.com/bu-ist/bux-grader-framework/).

## Features

* Grade MySQL problems with edX
* Rubric-based scoring to assign partial credit for incorrect submissions
* Generates hints for incorrect responses to help students identifiy errors
* Optionally uploads query results in CSV format to S3 bucket for download
* "Sandbox" mode allows students to experiment with ungraded problems

## Prerequisites

* A dedicated XQueue queue with authentication credentials
* MySQL
* Amazon S3 bucket and auth credneitals if CSV upload is required

## Quick Start

1. Add this repository to your course repositories `requirements.txt`:

```
git+https://github.com/bu-ist/bux-sql-grader.git@master#egg=bux-sql-grader
git+https://github.com/bu-ist/bux-grader-framework.git@master#egg=bux-grader-framework
```

2. Install the requirements with pip (use of `virtualenv` is highly recommended):

```bash
$ pip install -r requirements.txt
```

3. Add a settings module to for your course:

```python
# settings.py

# XQueue configuration
XQUEUE_QUEUE = "your-xqueue-queue"
XQUEUE_URL = "http://your-xqueue-host.com:18040"
XQUEUE_USER = "your-xqueue-user"
XQUEUE_PASSWORD = "your-xqueue-password"

# Evaluator configuration
EVALUATOR_MODULES = {
    "bux_sql_grader"
}

EVALUATOR_CONFIG = {
    "mysql": {
        "database": "default-database-name",
        "host": "your-db-hostname-here",
        "user": "your-db-username-here",
        "passwd": "your-db-passwd-here",
        "port": 3306,
        "s3_upload": True,
        "s3_bucket": "your-s3-bucket-name-here",
        "aws_access_key": "your-aws-access-key-here",
        "aws_secret_key": "your-aws-secret-key-here"
    }
}
```

4. Start the grader:

```python
grader --settings=settings
```

See the [demo course repository](https://github.com/bu-ist/bux-demo-course-grader) for an example course configuration. See the [configuration repository](https://github.com/bu-ist/bux-grader-configuration) for a more automated way to set up your grader environment.

## Contributing

Pull requests are welcome!

Pull down this repository and use `pip` to install development requirements:

```bash
$ git clone git@github.com:bu-ist/bux-sql-grader
$ pip install -r requirements.txt
```

### Documentation

Follow [pep257](http://legacy.python.org/dev/peps/pep-0257/)!

Package documentation lives in the `docs` directory and can be built in a variety of formats using [sphinx](http://sphinx-doc.org/).

```bash
cd docs
make html
```

The build directory (`docs/_build`) is excluded from VCS.

### Tests

All unit tests live in the `tests` directory and can be run using [nose](https://nose.readthedocs.org/en/latest/).

```bash
$ nosetests
```

### Coding Style

Follow [pep8](http://legacy.python.org/dev/peps/pep-0008/)!

Run [flake8](https://pypi.python.org/pypi/flake8) before you commit to make sure there aren't any violations:

```bash
$ flake8
```

