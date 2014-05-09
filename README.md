# edX SQL Grader

An [external grader](http://ca.readthedocs.org/en/latest/problems_tools/external_graders.html) for evaluation of SQL problems.

Designed for use with the [edX External Grader framework](https://github.com/bu-ist/bux-grader-framework/).

## Contributing

Pull requests are welcome!

Pull down this repository and use `pip` to install development requirements:

```bash
$ git clone git@github.com:bu-ist/bux-sql-grader
$ pip install -r dev-requirements.txt
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

