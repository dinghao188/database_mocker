from ply import lex

#keywords, type aka value
single_keywords = [
    "SELECT",
    "UPDATE",
    "SET",
    "FROM",
    "WHERE",
    "HAVING",
    "VALUES",
    "AS",
    "AND",
    "OR",
]

#combined keywords
conbined_keywords = [
    "INSERT_INTO",
    "ORDER_BY",
]
def t_INSERT_INTO(t):
    r'INSERT[ \t\n]+INTO'
    return t
def t_ORDER_BY(t):
    r'ORDER[ \t\n]+BY'
    return t


#function
function = [
    "MAX",
    "COUNT"
]
def t_MAX(t):
    r'MAX\(.+\)'
    t.value = ("MAX", t.value.split('(')[1].split(')')[0])
    return t
def t_COUNT(t):
    r'COUNT\(.+\)'
    t.value = ("COUNT", t.value.split("(")[1].split(")")[0])
    return t


#all tokens we need
tokens = [
    "IDENTIFIER", #basic identifier for table name, column name , etc.
    "INTEGER",    #literal interger
    "FLOAT",      #literal float
    "STRING",     #literal string
    "COMMA",      #,
    "COLON",      #:
    "STAR",       #*
    "LPAREN",     #(
    "RPAREN",     #)
    "EQ",         #=
    "NE",         #<>
    "LT",         #<
    "GT",         #>
    *single_keywords,
    *conbined_keywords,
    *function
]

t_ignore = ' \t\n'

t_COMMA = r','
t_COLON = r':'
t_STAR = r'\*'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_EQ = r'='
t_NE = r'<>'
t_LT = r'<'
t_GT = r'>'

def t_IDENTIFIER(t):
    r'[a-zA-Z_\*][a-zA-Z0-9_\.]*'
    t.type = "IDENTIFIER"
    if t.value in single_keywords:
        t.type = t.value
    return t
def t_INTEGER(t):
    r'\d+'
    t.value = int(t.value)
    return t
def t_FLOAT(t):
    r'\d+\.\d+'
    t.value = float(t.value)
    return t
def t_STRING(t):
    r'\'.*\''
    t.value = t.value.split("'")[1]
    return t

def t_error(t):
    print("Illegal character '{}'".format(t.value[0]))
    t.lexer.skip(1)