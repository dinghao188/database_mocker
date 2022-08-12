import database_mocker.ysql as ysql
from database_mocker.database import Database, Table, DBS
import database_mocker.api as api

def test_lexer1():
    ysql.lexer.input("SELECT * FROM SUBS")
    tok = True
    while tok:
        tok = ysql.lexer.token()
        print(tok)

def test_lexer2():
    ysql.lexer.input("""
    SELECT A.SUBS_ID, A.SUBS_SEQ AS NAME, A.ACCT_ID
      FROM SUBS A, ACCT, (SELECT * FROM PROD) C
     WHERE A.SUBS_ID = C.PROD_ID AND (A.ACCT_ID<>ACCT_ID OR A.ACCT_SEQ > 0) AND 1=1 OR 1=2
    """)
    tok = ysql.lexer.token()
    while tok:
        print(tok)
        tok = ysql.lexer.token()

def test_yacc1():
    stmt = ysql.parser.parse("""
    SELECT A.SUBS_ID, A.SUBS_SEQ AS NAME, A.ACCT_ID, 1 ID
      FROM SUBS A, ACCT, (SELECT * FROM PROD) C
     WHERE A.SUBS_ID = C.PROD_ID AND (A.ACCT_ID<>ACCT_ID OR A.ACCT_SEQ > 0) AND 1=1 OR 1=2 AND A.SUBS_SEQ=:SEQ
    """)
    assert isinstance(stmt, ysql.Select)
    
    print("COLUMNS =========>")
    for column in stmt.columns:
        print(column)
    print("TABLES ==========>")
    for table in stmt.tables:
        print(table)
    print("CONDS  ==========>")
    for cond_group in stmt.where:
        print(cond_group)

def test_insert():
    stmt = ysql.parser.parse("""
    INSERT INTO SUBS(SUBS_ID, SUBS_SEQ, SUBS_NAME) VALUES(1, 0, "dinghao")
    """)
    assert isinstance(stmt, ysql.Insert)
    
    print("COLUMNS =========>")
    for column in stmt.columns:
        print(column)
    print("TABLE ========> ", stmt.table)
    print("VALUES ==========>")
    for value in stmt.values:
        print(value)

def test_param():
    stmt = ysql.parser.parse("SELECT * FROM SUBS WHERE SUBS_ID = :ID")
    assert isinstance(stmt, ysql.Select)
    
    print("PARAMS =========>")
    print(stmt.params)
    
    stmt = ysql.parser.parse("SELECT * FROM PROD WHERE PROD_ID=:ID AND PROD_SEQ=:SEQ")
    print("PARAMS =========>")
    print(stmt.params)

def test_database():
    db = Database("CC")
    db.create_table("SUBS", ("SUBS_ID", "SUBS_NAME", "SUBS_SEQ"))
    db.create_table("PROD", ("PROD_ID", "OFFER_ID", "PROD_SEQ"))
    db.create_sequence("ID_SEQ", 0, 10)

    db["SUBS"].insert_record([
        (0, "dinghao", 0),
        (1, "dinghaoyan", 0)])
    db["PROD"].insert_record([
        (0, 0, 0),
        (1, 1, 0)
    ])

    print(db.execute("SELECT A.SUBS_ID FROM SUBS A"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT A.SUBS_NAME FROM SUBS A, PROD B WHERE A.SUBS_ID=B.PROD_ID"))
    print(db.execute("SELECT 1,2 FROM DUAL"))

def test_api():
    print("=====================test_api=============================")
    db = Database("CC")
    db.create_table("SUBS", ("SUBS_ID", "SUBS_NAME", "SUBS_SEQ"))
    db.create_table("PROD", ("PROD_ID", "OFFER_ID", "PROD_SEQ"))
    db.create_sequence("ID_SEQ", 0, 10)

    db["SUBS"].insert_record([
        (0, "dinghao", 0),
        (1, "dinghaoyan", 0)])
    db["PROD"].insert_record([
        (0, 0, 0),
        (1, 1, 0)
    ])
    DBS[db.name] = db

    si = api.Attach("CC")
    api.SetSQL(si, "SELECT A.SUBS_NAME FROM SUBS A WHERE A.SUBS_ID=:ID")
    api.SetParameterInteger(si, "ID", 1)
    api.Execute(si)
    print(api.FetchOne(si))
    api.Detach(si)
    
    si = api.Attach("CC")
    api.SetSQL(si, "SELECT PROD_ID FROM PROD WHERE PROD_SEQ=:SEQ OR PROD_ID=:ID")
    api.SetParameterInteger(si, "SEQ", 0)
    api.SetParameterInteger(si, "ID", 1)
    api.Execute(si)
    print(api.FetchOne(si))
    print(api.FetchOne(si))
    api.Detach(si)
    
    print("====> FetchOneWithOrder")
    si = api.Attach("CC")
    api.SetSQL(si, "SELECT SUBS_ID, SUBS_NAME FROM SUBS")
    api.Execute(si)
    print(api.FetchOneWithOrder(si))
    print(api.FetchOneWithOrder(si))
    api.Detach(si)
    print("======================================================")

def test_foo():
    db = Database("CC")
    db.create_table("OCS_SESSION", ("SESSION_ID", "EXT_ATTR"))
    db["OCS_SESSION"].insert_record({
        "SESSION_ID": "jfkdajfdsjalfjk",
        "EXT_ATTR": "something"
    })
    db["OCS_SESSION"].insert_record({
        "SESSION_ID": "abcdef",
    })
    print(db.execute("SELECT SESSION_ID, NVL(EXT_ATTR, 'nothing') EXT_ATTR FROM OCS_SESSION"))