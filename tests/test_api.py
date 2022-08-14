from database_mocker.database import *
import database_mocker.api as api

def test_api():
    print("=====================test_api=============================")
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