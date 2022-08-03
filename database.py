import database_mocker.ysql as ysql
import itertools
from typing import List,Dict

__all__ = ["Table", "Database", "Session", "DBS", "SESSIONS"]

class Table:
    def __init__(self, name, columns, records = None):
        self.name = name
        self.columns = columns
        self.records = records or []
    def insert_record(self, datas = []):
        self.records.extend(datas)
    def get_all_records(self):
        res = []
        for record in self.records:
            res.append(dict(zip(self.columns, record)))
        return res

class Sequence:
    def __init__(self, name, init_val, step):
        self.name = name
        self.val = init_val
        self.step = step
    def nextval(self) -> int:
        oldval = self.val
        self.val += self.step
        return oldval

class Database:
    def __init__(self, name):
        self.tables = {}
        self.sequences = {}
        self.name = name
    def create_table(self, table):
        self.tables[table.name] = table
    def create_sequence(self, sequence):
        self.sequences[sequence.name] = sequence

    def execute(self, sql: str):
        stmt = ysql.parser.parse(sql)
        if isinstance(stmt, ysql.Select):
            return self.execute_select(stmt)
        else:
            return "NoThing!!!!!!!!!!!!!!"
    def execute_select(self, select: ysql.Select):
        all_records = self.__fetech_data(select)

        __product_records = []
        for table, recs in all_records.items():
            __product_records.append([])
            for rec in recs:
                __product_records[-1].append({ table: rec })
        def record_product_to_dict(product: tuple[Dict]):
            ans = {}
            for factor in product:
                ans.update(factor)
            return ans
        product_records_generator = map(record_product_to_dict, itertools.product(*__product_records, repeat=1))
        
        ret_records = []
        for rec in product_records_generator:
            if self.__where_filter(select.where, rec):
                ret_rec = {}
                for colname, colref in select.columns.items():
                    if isinstance(colref, int):
                        ret_rec[colname] = str(colref)
                        continue
                    tmp = colref.split(".")
                    colref_entity = None
                    colref_field = None
                    if len(tmp) == 1:
                        colref_field = tmp[0]
                        colref_entity = None
                    else:
                        colref_entity = tmp[0]
                        colref_field = tmp[1]
                    print("##########", colref_entity, colref_field)
                    #SEQ.NEXTVAL
                    if colref.endswith(".NEXTVAL"):
                        ret_rec[colname] = str(self.sequences[colref_entity].nextval())
                    #TABLE.COLUMN
                    else:
                        if colref_entity is not None:
                            ret_rec[colname] = str(rec[colref_entity][colref_field])
                ret_records.append(ret_rec)
        return ret_records

    def __fetech_data(self, select: ysql.Select):
        """
        根据sql中的表名获取所有数据,结果形式如下
        {
            "TABLE1": [data1, data2],
            "TABLE2": [data3, data4]
        }
        其中TABLEx表示表的别名(优先)或者表名,
            datax表示以键值对形式表示的表记录
        """

        all_records = {}
        for table_alias in select.tables.keys():
            table = select.tables[table_alias]
            if table == "DUAL":
                all_records["DUAL"] = [{}]
                continue
            if isinstance(table, ysql.Select):
                all_records[table_alias] = self.execute_select(self, table)
            else:
                all_records[table_alias] = self.tables[table].get_all_records()
        return all_records
    def __where_filter(self, where: List[ysql.CondGroup], data: Dict):
        """
        根据sql中的where条件对数据进行过滤，其中data的格式参考函数__fetch_data
        @return True: 该数据满足where条件
        @return False: 该数据不满足where条件
        """
        
        # 没有条件的话直接返回
        if len(where) == 0:
            return True
        # 对判断条件的简单映射
        cmp_funcs = {
            "=": lambda left, right: left == right,
            ">": lambda left, right: left > right,
            "<": lambda left, right: left < right,
            "<>": lambda left, right: left != right,
            "IS": lambda left, right: left is right,
            "IS NOT": lambda left, right: left is not right
        }
        # 提取条件列的值: 因为条件列可能有多种形式，1. 数字 2. 字符串 3. 表字段
        def extract_cond_value(cond_column):
            if isinstance(cond_column, int):
                return cond_column
            elif cond_column.startswith("'") and cond_column.endswith("."):
                return cond_column[0:-1]
            else:
                cond_column = cond_column.split(".")
                if len(cond_column) == 1:
                    cond_column = [list(data.keys())[0], cond_column[0]]
                return data[cond_column[0]][cond_column[1]]

        for cond_group in where:
            match = True
            for cond in cond_group.conds:
                if cond.cond_groups is not None:
                    match = match and self.__where_filter(cond.cond_groups, data)
                else:
                    left_value = extract_cond_value(cond.left)
                    right_value = extract_cond_value(cond.right)
                    match = match and cmp_funcs[cond.op](left_value, right_value)
                if not match:
                    break
            if match:
                return True
        return False

    def __getitem__(self, table_name) -> Table:
        return self.tables[table_name]

class Session:
    def __init__(self, session_id, db_name):
        self.session_id = session_id
        self.db_name = db_name

        self.stmt = None
        self.cursor_data = None
        self.cursor_pos = 0
    def clear(self):
        self.stmt = None
        self.cursor_data = None
        self.cursor_pos = 0
    def fetchone(self):
        if self.cursor_data is None or self.cursor_pos >= len(self.cursor_data):
            return None
        data = self.cursor_data[self.cursor_pos]
        self.cursor_pos += 1
        return data

            
DBS: Dict[str, Database] = {}
SESSIONS: Dict[int, Session] = {}


if __name__ == '__main__':
    db = Database("CC")
    db.create_table(Table(
        "SUBS",
        ("SUBS_ID", "SUBS_NAME", "SUBS_SEQ")))
    db.create_sequence(Sequence(
        "ID_SEQ", 1, 10
    ))
    db["SUBS"].insert_record([
        (1, "dinghao", 0),
        (2, "dinghaoyan", 0)])
    print(db.execute("SELECT A.SUBS_ID FROM SUBS A"))
    print(db.execute("SELECT ID_SEQ.NEXTVAL FROM DUAL"))