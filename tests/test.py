import database_mocker.ysql as ysql
from database_mocker.database import Database, Table, DBS
import database_mocker.api as api

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