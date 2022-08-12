from typing import List, Dict
from ply import lex, yacc

"""-----------------for lex--------------------"""
import database_mocker.common_lex
lexer = lex.lex(module=database_mocker.common_lex)
from database_mocker.common_lex import tokens

"""-----------------for yacc-------------------"""
from ply import lex, yacc

class Column:
    COLUMN_TYPE_LITERAL = 0
    COLUMN_TYPE_ENTITY = 1
    COLUMN_TYPE_FUNCTION = 2

    def __init__(self, name, value, type):
        self.name = name
        self.value = value
        self.type = type
    def __str__(self):
        return "{}, {}".format(self.name, self.value)

class Function:
    def __init__(self, name, params: List = None):
        self.name = name
        self.params = params

class Table:
    TABLE_TYPE_TABLE = 0
    TABLE_TYPE_SUBSELECT = 1

    def __init__(self, name, ent, type):
        self.name = name
        self.ent = ent
        self.type = type
    def __str__(self):
        return "{}, {}".format(self.name, self.ent)

class Value:
    VALUE_TYPE_LITERAL = 0
    VALUE_TYPE_COLUMN = 1
    VALUE_TYPE_PARAM = 2
    def __init__(self, ent, type):
        self.ent = ent
        self.type = type
    def __str__(self):
        return str(self.ent)

class CondOperand:
    COND_OPERAND_TYPE_LITERAL = 0
    COND_OPERAND_TYPE_ENTITY = 1
    COND_OPERAND_TYPE_PARAM = 2
    def __init__(self, ent, type):
        self.ent = ent
        self.type = type
    def __str__(self):
        return str(self.ent)
class Cond:
    def __init__(self, left: Value = None, op = None, right: Value = None, cond_groups = None):
        self.left = left
        self.right = right
        self.op = op
        self.cond_groups: List[CondGroup] = cond_groups
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
    def __init__(self, columns: List[Column], tables: List[Table], where: List[CondGroup] = None, params: List[str] = None):
        self.columns = columns or []
        self.tables = tables or []
        self.where = where or []
        self.params = params or []
    def fetch_data(self):
        pass
    
class Insert:
    def __init__(self, table: str, columns: List[Column], values: List[Value], params: List[str] = None):
        self.table = table
        self.columns = columns
        self.values = values
        self.params = params or []

# 解析某个SQL获得的参数
params = {}

def p_statement(p):
    """
    statement : select
              | insert
    """
    p[0] = p[1]
    p[0].params = params.copy()
    params.clear()
def p_insert(p):
    """
    insert : INSERT_INTO table LPAREN columns RPAREN VALUES LPAREN values RPAREN
    """
    table = p[2]
    columns = p[4]
    values = p[8]
    p[0] = Insert(table, columns, values)
def p_values(p):
    """
    values : value
           | values COMMA value
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1]
        p[0].append(p[3])
def p_literal_value(p):
    """
    value : INTEGER
          | FLOAT
          | STRING
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_LITERAL)
def p_column_value(p):
    """
    value : IDENTIFIER
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_COLUMN)
def p_param_value(p):
    """
    value : param
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_PARAM)
def p_select(p):
    """
    select : SELECT columns FROM tables
           | SELECT columns FROM tables WHERE conds
    """
    if len(p) == 5:
        p[0] = Select(p[2], p[4])
    elif len(p) == 7:
        p[0] = Select(p[2], p[4], p[6])
def p_columns(p):
    """
    columns : column
            | columns COMMA column
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1]
        p[0].append(p[3])
def p_column(p):
    """
    column : column_v
           | column_v IDENTIFIER
           | column_v AS IDENTIFIER
    """
    p[0] = p[1]
    if len(p) == 2:
        pass
    # SUBS_ID ID or SUBS.SUBS_ID ID
    elif len(p) == 3:
        p[1].name = p[2]
    # SUBS_ID AS ID or SUBS.SUBS_ID as ID
    elif len(p) == 4:
        p[1].name = p[3]
def p_normal_column(p):
    """
    column_v : IDENTIFIER
    """
    p[0] = Column(p[1].split('.')[-1], p[1], Column.COLUMN_TYPE_ENTITY)
def p_literal_column(p):
    """
    column_v : INTEGER
             | FLOAT
             | STRING
    """
    p[0] = Column(str(p[1]), p[1], Column.COLUMN_TYPE_LITERAL)
def p_function_column(p):
    """
    column_v : function
    """
    p[0] = Column(p[1].name, p[1], Column.COLUMN_TYPE_FUNCTION)
def p_tables(p):
    """
    tables : table
           | tables COMMA table
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1]
        p[0].append(p[3])
def p_table(p):
    """
    table : IDENTIFIER
          | IDENTIFIER IDENTIFIER
    """
    if len(p) == 2:
        p[0] = Table(p[1].split('.')[-1], p[1], Table.TABLE_TYPE_TABLE)
    else:
        p[0] = Table(p[2], p[1], Table.TABLE_TYPE_TABLE)
def p_subselect_table(p):
    """
    table : LPAREN select RPAREN
          | LPAREN select RPAREN IDENTIFIER
    """
    if len(p) == 4:
        p[0] = Column("__SUBSELECT__", p[2], Table.TABLE_TYPE_SUBSELECT)
    else:
        p[0] = Column(p[4], p[2], Table.TABLE_TYPE_SUBSELECT)
def p_where(p):
    """
    where : WHERE conds
    """
    p[0] = p[2]
def p_conds(p):
    """
    conds : cond
          | conds AND cond
          | conds OR cond
    """
    if len(p) == 2:
        p[0] = [CondGroup(p[1]),]
    else:
        p[0] = p[1]
        if p[2] == "AND":
            p[0][-1].append(p[3])
        elif p[2] == "OR":
            p[0].append(CondGroup(p[3]))
def p_cond(p):
    """
    cond : cond_operand EQ cond_operand
         | cond_operand NE cond_operand
         | cond_operand LT cond_operand
         | cond_operand GT cond_operand
    """
    p[0] = Cond(left=p[1], op=p[2], right=p[3])
def p_cond_combine(p):
    """
    cond : LPAREN conds RPAREN
    """
    p[0] = Cond(cond_groups=p[2])
def p_cond_operand_literal(p):
    """
    cond_operand : STRING
                 | INTEGER
                 | FLOAT
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_LITERAL)
def p_cond_operand_entity(p):
    """
    cond_operand : IDENTIFIER
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_COLUMN)
def p_cond_operand_param(p):
    """
    cond_operand : param
    """
    p[0] = Value(p[1], Value.VALUE_TYPE_PARAM)
def p_param(p):
    """
    param : COLON IDENTIFIER
    """
    params[p[2]] = None
    p[0] = p[2]
def p_function(p):
    """
    function : func_nvl
    """
    p[0] = p[1]
def p_func_nvl(p):
    """
    func_nvl : NVL LPAREN IDENTIFIER COMMA column_v RPAREN
    """
    p[0] = Function("NVL", [p[3], p[5]])
def p_error(p):
    pass

parser = yacc.yacc()