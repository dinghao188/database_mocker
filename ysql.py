from typing import Dict, List

"""-----------------for lex--------------------"""
from ply import lex
import database_mocker.common_lex
lexer: lex.Lexer = lex.lex(module=database_mocker.common_lex)

"""-----------------for yacc------------------"""
from ply import yacc

class Column:
    COLUMN_TYPE_LITERAL = 0
    COLUMN_TYPE_ENTITY = 1

    def __init__(self, name, value, type):
        self.name = name
        self.value = value
        self.type = type

class Table:
    TABLE_TYPE_DUAL = 0
    TABLE_TYPE_TABLE = 1
    TABLE_TYPE_SUBSELECT = 2

    def __init__(self, name, ent, type):
        self.name = name
        self.ent = ent
        self.type = type

class Cond:
    def __init__(self, left, op, right, cond_groups = None):
        self.left = left
        self.right = right
        self.op = op
        self.cond_groups: List[CondGroup] = cond_groups #[CondGroup]
    def __str__(self):
        if self.cond_groups is not None:
            return "({})".format(" OR ".join([str(cg) for cg in self.cond_groups]))
        else:
            return "{} {} {}".format(self.left, self.op, self.right)

class CondGroup:
    def __init__(self, *conds):
        self.conds: List[Cond] = [*conds]
    def append(self, cond):
        self.conds.append(cond)
    def __str__(self):
        return " AND ".join([str(cond) for cond in self.conds])

class Select:
    def __init__(self, columns: List[Column], tables: List[Table], where = List[CondGroup]):
        self.columns = columns or []
        self.tables = tables or []
        self.where = where or []
    def fetch_data(self):
        pass



def p_statement(p):
    """statement : select"""
    p[0] = p[1]
def p_select(p):
    """select : basic_select
              | select_where
              | select_orderby"""
    p[0] = p[1]
def p_basic_select(p):
    """basic_select : SELECT select_columns FROM tables"""
    se = Select(p[2], p[4])
    p[0] = se
def p_select_where(p):
    """select_where : select WHERE conds"""
    p[0] = p[1]
    p[0].where = p[3]
def p_select_orderby(p):
    """select_orderby : basic_select ORDERBY ENTITY
                      | select_where ORDERBY ENTITY"""
    pass

# WHERE A.COLUMN = B.COLUMN AND (A.COLUMN > C.COLUMN OR B.COLUMN > C.COLUMN)
def p_conds(p):
    """conds : cond
             | conds AND cond
             | conds OR cond"""
    if len(p) == 2:
        p[0] = [CondGroup(p[1]),]
    else:
        p[0] = p[1]
        if p[2] == "AND":
            p[0][-1].append(p[3])
        elif p[2] == "OR":
            p[0].append(CondGroup(p[3]))

def p_cond_entity(p):
    """cond_entity : ENTITY
                   | NUMBER
                   | STRING"""
    p[0] = p[1]
def p_basic_cond(p):
    """cond : cond_entity EQ cond_entity
            | cond_entity NE cond_entity
            | cond_entity LT cond_entity
            | cond_entity GT cond_entity"""
    p[0] = Cond(p[1], p[2], p[3], None)
def p_combine_cond(p):
    """cond : LPAREN conds RPAREN"""
    p[0] = Cond("", "", "", p[2])
def p_select_columns(p):
    """select_columns : column
                      | select_columns COMMA column"""
    if len(p) == 2:
        p[0] = { p[1][0]: p[1][1] }
    else:
        p[1][p[3][0]] = p[3][1]
        p[0] = p[1]
def p_column(p):
    """column : ENTITY
              | ENTITY ENTITY
              | ENTITY AS ENTITY"""
    def get_col_name(s):
        sp = s.split(".")
        if len(sp) == 1:
            return sp[0]
        else:
            return sp[1]
    if len(p) == 2:
        p[0] = (get_col_name(p[1]), p[1])
    elif len(p) == 3:
        p[0] = (get_col_name(p[2]), p[1])
    elif len(p) == 4:
        p[0] = (get_col_name(p[3]), p[1]) 
def p_literal_column(p):
    """column : NUMBER
              | STRING
              | NUMBER ENTITY
              | NUMBER AS ENTITY
              | STRING ENTITY
              | STRING AS ENTITY"""
    if len(p) == 2:
        p[0] = (str(p[1]), p[1])
    elif len(p) == 3:
        p[0] = (str(p[2]), p[1])
    elif len(p) == 4:
        p[0] = (str(p[3]), p[1])
def p_tables(p):
    """tables : table
              | tables COMMA table
              | subselect
              | tables COMMA subselect"""
    if len(p) == 2:
        p[0] = { p[1][0]: p[1][1] }
    else:
        p[1][p[3][0]] = p[3][1]
        p[0] = p[1]
def p_table(p):
    "table : ENTITY"
    p[0] = ( p[1], p[1] )
def p_table_alias(p):
    "table : ENTITY ENTITY"
    p[0] = ( p[2], p[1] )
def p_subselect(p):
    "subselect : LPAREN select RPAREN"
    p[0] = ( "__NOALIAS__", p[2] )
def p_subselect_alias(p):
    "subselect : LPAREN select RPAREN ENTITY"
    p[0] = ( p[4], p[2] )

# parser = yacc.yacc()

__all__ = ["Select", "Cond", "CondGroup", "parser"]