#MySQL Package#

This package allows the import and export of MySQL tables to/from csv files.

##Importing##

To import tables into your MySQL database from you just need to import the package
and instantiate the class with your MySQL connection string as below. You also need to pass
in the path where the source csv's live

```python

from mysql.ImportMySQL import *

mysql = ImportMySQL(host, user, passwd, dbname, path)
mysql.import_dir()

```

