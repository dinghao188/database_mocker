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

def test_yacc_insert():
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

def test_function():
    stmt = ysql.parser.parse("SELECT TO_DATE(:SUBS_ID, '123213213'), NVL(:SUBS_NAME, '') FROM SUBS")
    print("COLUMNS =========>")
    for column in stmt.columns:
        print(column)