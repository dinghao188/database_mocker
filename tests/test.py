def test_foo():
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
