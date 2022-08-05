import database_mocker.ysql as ysql
from database_mocker.database import Database, Table, Sequence

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
     WHERE A.SUBS_ID = C.PROD_ID AND (A.ACCT_ID<>ACCT_ID OR A.ACCT_SEQ > 0) AND 1=1 OR 1=2
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


def test_database():
    db = Database("CC")
    db.create_table(Table(
        "SUBS",
        ("SUBS_ID", "SUBS_NAME", "SUBS_SEQ")))
    db.create_table(Table(
        "PROD",
        ("PROD_ID", "OFFER_ID", "PROD_SEQ")))
    db.create_sequence(Sequence(
        "ID_SEQ", 0, 10
    ))
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