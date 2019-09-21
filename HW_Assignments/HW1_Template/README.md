# W4111_F19_HW1
Hans Montero  
hjm2133


**GENERAL OVERVIEW**
- CSV and RDB DataTable work as expected. They both behave exactly the same,
and to confirm this, their unit tests are the exact same code.

- Most of the operations rely on the idea of a "primary key", that is, a unique identifier for each row. This is meant to maintain integrity of our data. All methods ensure that the primary key is not duplicated.
- Two features of both DataTables are the ability to search by a "template" and to only return certain "fields". SQL is perfect for this kind of thing! "Templates" are really just "conditions" within the WHERE
clause that the row must match. "Fields" are just column names specified in the SELECT clause.
If no fields are specified, the statement is equivalent to `SELECT * ...`.

**DESIGN CHOICES**
- I made `HW1_Template` the root of my PyCharm project.
- CSVDataTable's `_add_row()` method was left as it was in the original template.
It would simply take forever to perform integrity checks with it since we are reading in relatively large CSV files.
    - Instead, `insert()` performs the necessary integrity checks (row contains valid primary key, no dups, etc).
    - `insert()` should be used to perform any inserts after the table is created.
- CSVDataTable will throw exceptions if modification methods (insert/delete) by primary key
have missing/invalid keys.
- CSVDataTable key methods will throw exceptions if the table wasn't initialized with primary keys.


- RDBDataTable will *NOT* commit by default. I think this makes testing easier and less stateful.
`commit=True` should be passed into the constructor to change this behavior.
- Almost all error checking has been delegated to MySQL, which will throw errors as needed.
- `get_rows()` has been modified to *NOT* use `self._rows`. I think this doesn't make sense
for an object that depends on a server to get its values. Instead, this method will now execute `SELECT * FROM <table> WHERE 1`.

