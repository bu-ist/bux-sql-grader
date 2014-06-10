# Changelog

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
