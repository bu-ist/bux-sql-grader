# Changelog

## 0.4.2

* Ensure MySQL port parameters are passed as integers
* Add version constraints in setup.py

## 0.4.1

* Open source release
* Make S3 uploads optional

## 0.4.0

* Corresponds with 0.4.0 release of bux-grader-framework
* Removes LXML message valiadtion (moved to the framework)
* Adds failure hints for framework to display when evaluation fails unexpectedly

## 0.3.3

* Adds MAX_QUERY_LENGTH to prevent execution of unrealistically large queries (10,000+ characters)

## 0.3.2

* Switches DOUBLE from unicode -> float in custom MySQLdb converter dict. Preserves current behavior / fixes precision issue with R problems.

## 0.3.1

* Adds MySQL date types to custom MySQLdb.converter
* Adds support for multiple LIMIT clauses within a single statement
* Removes unneccessary escaping of upload error message

## 0.3.0

* Adds automatic query LIMITs via the `select_limit` arg
* Adds custom MySQLdb converter to optimize scoring methods

## 0.2.9

* Removed custom MySQLdb.converter due to comparison / formatting issues
* Adds closing of DB cursors / connections
* Adds cleanup method to scoring class to free memory

## 0.2.8

* Scoring optimizations

## 0.2.7

* Adds support for unicode characters in result rows / column headings
* Prevents UnicodeDecodeError / UnicodeEncodeError exceptions

## 0.2.6

* Hotfix: Add XML sanitization for column headings

## 0.2.5

* Adds SET to the SQL blacklist to prevent students from modifying session variables
* Adds more robust XML sanitization / validation for response message to prevent XML parse errors in the LMS
* Adds dependency on lxml
