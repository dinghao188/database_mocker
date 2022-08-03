from typing import List,Dict
from database_mocker.database import *
import database_mocker.ysql as ysql

__all__ = ["Attach", "Detach", "SetSQL", "Execute", "FetchOne"]

__session_id = 1

def Attach(db_name) -> int:
    global __session_id
    print("Attach", __session_id, DBS)
    if db_name not in DBS.keys(): 
        return 0
    SESSIONS[__session_id] = Session(__session_id, db_name)
    old_id = __session_id
    __session_id += 1
    return old_id
def Detach(session_id: int):
    print("Detach", session_id)
    print("Detach", SESSIONS.keys())
    if session_id in SESSIONS.keys():
        SESSIONS.pop(session_id)
def SetSQL(session_id: int, sql: str) -> List[str]:
    ans = []
    if session_id in SESSIONS.keys():
        session = SESSIONS[session_id]
        session.clear()
        session.stmt = ysql.parser.parse(sql)
        for column in session.stmt.columns:
            column_tmp = column.split(".")
            if len(column_tmp) == 2:
                ans.append(column_tmp[1])
            else:
                ans.append(column_tmp[0])
    return ans
def Execute(session_id: int) -> int:
    if session_id not in SESSIONS.keys():
        return 1
    session = SESSIONS[session_id]
    if session.stmt is None:
        return 2
    session.cursor_data = DBS[session.db_name].execute_select(session.stmt)
    return 0
def FetchOne(session_id: int) -> Dict[str, str]:
    if session_id not in SESSIONS.keys():
        return {}
    session = SESSIONS[session_id]
    record = session.fetchone() or {}
    return record