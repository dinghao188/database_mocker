from typing import List,Dict,Union
from database_mocker.database import *
import database_mocker.ysql as ysql

__all__ = ["Attach", "Detach", "SetSQL", "Execute", "FetchOne"]

__session_id = 1

def Attach(db_name) -> int:
    global __session_id
    print("Attach", db_name, __session_id, DBS)
    if db_name not in DBS.keys(): 
        return 0
    SESSIONS[__session_id] = Session(__session_id, db_name)
    old_id = __session_id
    __session_id += 1
    return old_id
def Detach(session_id: int):
    if session_id in SESSIONS.keys():
        SESSIONS.pop(session_id)
def SetSQL(session_id: int, sql: str) -> List[str]:
    ans = []
    if session_id not in SESSIONS.keys():
        return ans
    session = SESSIONS[session_id]
    session.clear()
    session.stmt = ysql.parser.parse(sql.upper())
    if isinstance(session.stmt, ysql.Select):
        for column in session.stmt.columns:
            ans.append(column.name)
    return ans
def Execute(session_id: int) -> int:
    import traceback
    try:
        print("Execute", session_id, DBS, SESSIONS)
        if session_id not in SESSIONS.keys():
            return 1
        session = SESSIONS[session_id]
        if session.stmt is None:
            return 2
        if isinstance(session.stmt, ysql.Select):
            session.cursor_data = DBS[session.db_name].execute_select(session.stmt)
            return 0
        elif isinstance(session.stmt, ysql.Insert):
            return DBS[session.db_name].execute_insert(session.stmt)
    except Exception as e:
        traceback.print_exc()
        return 3
def FetchOne(session_id: int) -> Dict[str, Union[str, int]]:
    if session_id not in SESSIONS.keys():
        return {}
    session = SESSIONS[session_id]
    record = session.fetchone() or {}
    return record
def FetchOneWithOrder(session_id: int) -> List[Union[str, int]]:
    if session_id not in SESSIONS.keys():
        return []
    session = SESSIONS[session_id]
    record = session.fetchone_with_order() or []
    return record

def __SetParameter(session_id: int, param: str, val: Union[int,float,str,None]):
    if session_id not in SESSIONS.keys():
        return False
    session = SESSIONS[session_id]
    if param not in session.stmt.params.keys():
        return False
    session.stmt.params[param] = val
    return True

def SetParameterNULL(session_id: int, param: str):
    __SetParameter(session_id, param, None)
def SetParameterDouble(session_id: int, param: str, val: float):
    __SetParameter(session_id, param, val)
def SetParameterInteger(session_id: int, param: str, val: int):
    __SetParameter(session_id, param, val)
def SetParameterString(session_id: int, param: str, val: str):
    __SetParameter(session_id, param, val)