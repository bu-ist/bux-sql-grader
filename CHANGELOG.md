# Changelog

## 0.2.7

* Adds support for unicode characters in result rows / column headings
* Prevents UnicodeDecodeError / UnicodeEncodeError exceptions

## 0.2.6

* Hotfix: Add XML sanitization for column headings

## 0.2.5

* Adds SET to the SQL blacklist to prevent students from modifying session variables
* Adds more robust XML sanitization / validation for response message to prevent XML parse errors in the LMS
* Adds dependency on lxml
