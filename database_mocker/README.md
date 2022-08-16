# DATABASE_MOCKER

## What is this
This is a simple database mocker which can be used for unit test.

- supported features
  - insert *TODO*
  - update *TODO*
  - select
    - table alias
    - column alias
    - subselect
    - name parameter
    - function *TODO*
  - sequence

## Dependencies

- **ply** is used to parse sql  
- **pytest** is used for unit test of this project


## Example

```python
import database_mocker.api as api
from database_mocker.database import Database

# 1. first create a database called TESTDB, and
db = Database("TESTDB")
# then create a table named PERSON, and with bellow columns
db.create_table("PERSON", ("ID", "NAME", "AGE", "PHONE"))

# 2. insert some data into this table
# this methods must specify all values of every column
db["PERSON"].insert_records([
  (1, "Jack", 18, "999"),
  (2, "Mary", 20, "12389232"),
  (3, "Tom", 35, None)
])
# or you can try to use this method to insert a single record with specified column
#      value of columns which are not specified will be None
db["PERSON"].insert_record({
  "ID": 4,
  "NAME": "Tony",
})

# 3. use api to operate this database
## 3.1 attach to database and acquire a unique session_id
session_id = api.Attach("TESTDB")
## 3.2 prepare a sql you want
api.SetSQL(session_id, "SELECT ID PERSON_ID, NAME, AGE, PHONE FROM PERSON")
## 3.3 execute, this will fetch all data and store it in current session
api.Execute(session_id)
## 3.4 fetch data, FetchOne will return a key value data, until cursor at the end, an empty Dict will be returned
print(api.FetchOne(session_id))
print(api.FetchOne(session_id))
print(api.FetchOne(session_id))
print(api.FetchOne(session_id))
print(api.FetchOne(session_id))
```

you will get output like below:

```
{'PERSON_ID': 1, 'NAME': 'Jack', 'AGE': 18, 'PHONE': '999'}
{'PERSON_ID': 2, 'NAME': 'Mary', 'AGE': 20, 'PHONE': '12389232'}
{'PERSON_ID': 3, 'NAME': 'Tom', 'AGE': 35, 'PHONE': None}
{'PERSON_ID': 4, 'NAME': 'Tony', 'AGE': None, 'PHONE': None}
{}
```