import database_mocker.ysql as ysql
from database_mocker.database import Database, Table, DBS
import database_mocker.api as api

def test_foo():
    stmt = ysql.parser.parse("""
    SELECT SESSION_ID,
           NVL(RESERVE_TIMES,-1) RESERVE_TIMES,
           EXP_TIME,
           SERV_BEGIN_TIME,
           NVL(SUBS_ID,-1) SUBS_ID,
           NVL(UNIT_RESERVE,-1) UNIT_RESERVE,
           NVL(UNIT_TYPE,-1) UNIT_TYPE,
           NVL(UNIT_USED,-1) UNIT_USED,
           NVL(PID_INFO,'') PID_INFO,
           NVL(RATING_FLOW_ID,-1) RATING_FLOW_ID,
           NVL(EXT_ATTR,'') EXT_ATTR,
           NVL(CCR_NUMBER,-1) CCR_NUMBER
    FROM OCS_SESSION
    WHERE SESSION_ID = :SESSION_ID
    """)

    print("COLUMNS========>")
    for col in stmt.columns:
        print(col)
