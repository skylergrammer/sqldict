# Contents
* sqldict.py

# Function/Objects:
*SqlDict -- dict-like object that will use a sql table to perform key lookups.

*make_sql_table -- function that will take a list of (key, value) pairs and create a sql table in the format used by SqlDict.

# Usage
```python
database_name = "test_database.db"
sql_dict = SqlDict(database_name)
print(sql_dict["key"])
>>> value
```