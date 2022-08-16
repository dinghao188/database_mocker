import database_mocker.ysql as ysql

def test_lexer_select():
    ysql.lexer.input("SELECT * FROM SUBS")
    tok = True
    while tok:
        tok = ysql.lexer.token()
        print(tok)

def test_lexer_select_inner():
    ysql.lexer.input("""
    SELECT A.SUBS_ID, A.SUBS_SEQ AS NAME, A.ACCT_ID
      FROM SUBS A, ACCT, (SELECT * FROM PROD) C
     WHERE A.SUBS_ID = C.PROD_ID AND (A.ACCT_ID<>ACCT_ID OR A.ACCT_SEQ > 0) AND 1=1 OR 1=2
    """)
    tok = ysql.lexer.token()
    while tok:
        print(tok)
        tok = ysql.lexer.token()

def test_yacc_select():
    sql = """
    SELECT A.SUBS_ID, A.SUBS_SEQ AS NAME, A.ACCT_ID, 1 ID
      FROM SUBS A, ACCT, (SELECT * FROM PROD) C
     WHERE A.SUBS_ID = C.PROD_ID AND (A.ACCT_ID<>ACCT_ID OR A.ACCT_SEQ > 0) AND 1=1 OR 1=2 AND A.SUBS_SEQ=:SEQ
    """
    stmt = ysql.parser.parse(sql)
    assert isinstance(stmt, ysql.Select)
    
    print(sql)
    print("COLUMNS ==>", [str(col) for col in stmt.columns])
    print("TABLES  ==>", [str(tab) for tab in stmt.tables])
    print("CONDS   ==>", stmt.where)

def test_yacc_insert():
    sql = "INSERT INTO SUBS(SUBS_ID, SUBS_SEQ, SUBS_NAME) VALUES(1, 0, 'dinghao')"
    stmt = ysql.parser.parse(sql)
    assert isinstance(stmt, ysql.Insert)
    
    print(sql)
    print("COLUMNS ==>", stmt.columns)
    print("TABLE   ==> ", stmt.table)
    print("VALUES  ==>", [str(v) for v in stmt.values])

def test_param():
    sql = "SELECT * FROM SUBS WHERE SUBS_ID = :ID"
    stmt = ysql.parser.parse(sql)
    assert isinstance(stmt, ysql.Select)
    
    print(sql)
    print("PARAMS ==>", stmt.params)
    
    sql = "SELECT * FROM PROD WHERE PROD_ID=:ID AND PROD_SEQ=:SEQ"
    stmt = ysql.parser.parse(sql)
    assert isinstance(stmt, ysql.Select)

    print(sql)
    print("PARAMS ==>", stmt.params)

def test_function():
    sql = "SELECT TO_DATE(SUBS_ID, '123213213') , NVL(:SUBS_NAME, '') SUBS_NAME FROM SUBS"
    stmt = ysql.parser.parse(sql)
    assert isinstance(stmt, ysql.Select)
    
    print(sql)
    print("COLUMNS ==>", [str(col) for col in stmt.columns])