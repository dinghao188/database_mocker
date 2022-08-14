from database_mocker.database import *

def test_database():
    db = Database("CC")
    db.create_table("SUBS", ("SUBS_ID", "SUBS_NAME", "SUBS_SEQ"))
    db.create_table("PROD", ("PROD_ID", "OFFER_ID", "PROD_SEQ"))
    db.create_sequence("ID_SEQ", 0, 10)

    db["SUBS"].insert_records([
        (0, "dinghao", 0),
        (1, "dinghaoyan", 0)])
    db["PROD"].insert_records([
        (0, 0, 0),
        (1, 1, 0)
    ])

    print(db.execute("SELECT A.SUBS_ID FROM SUBS A"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))
    print(db.execute("SELECT A.SUBS_NAME FROM SUBS A, PROD B WHERE A.SUBS_ID=B.PROD_ID"))
    print(db.execute("SELECT 1,2 FROM DUAL"))