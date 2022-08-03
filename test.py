import database_mocker.ysql as ysql

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