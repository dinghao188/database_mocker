import database_mocker.ysql as ysql
import itertools
from typing import List,Dict,Tuple,Union

__all__ = ["Table", "Database", "Session", "DBS", "SESSIONS"]

class Table:
    def __init__(self, name: str, columns: Tuple[str], records: List[Tuple] = None):
        self.name = name
        self.columns = columns
        self.records = records or []
    def clear(self):
        self.records = []
    def insert_record(self, data: Dict[str, Union[str,int]]):
        record = []
        for col in self.columns:
            if col in data.keys():
                record.append(data[col])
            else:
                record.append(None)
        self.records.append(record)
    def get_all_records(self):
        res = []
        for record in self.records:
            res.append(dict(zip(self.columns, record)))
        return res

class Sequence:
    def __init__(self, name: str, init_val: int, step: int):
        self.name = name
        self.val = init_val
        self.__step = step
        self.__init_val = init_val
    def clear(self): 
        self.val = self.__init_val
    def nextval(self) -> int:
        oldval = self.val
        self.val += self.__step
        return oldval

class Database:
    def __init__(self, name: str):
        self.tables : Dict[str, Table] = {}
        self.sequences : Dict[str, Sequence] = {}
        self.name = name
    #Clear all data, but keep all tables and sequences
    def clear(self):
        for table in self.tables.values():
            table.clear()
        for seq in self.sequences.values():
            seq.clear()
    def create_table(self, table_name: str, columns: Tuple[str], data: List[Tuple] = None):
        self.tables[table_name] = Table(table_name, columns, data)
    def create_sequence(self, name: str, init_val: int, step: int):
        self.sequences[name] = Sequence(name, init_val, step)

    def execute(self, sql: str):
        stmt = ysql.parser.parse(sql)
        if isinstance(stmt, ysql.Select):
            return self.execute_select(stmt)
        elif isinstance(stmt, ysql.Insert):
            return self.execute_insert(stmt)
        else:
            return "NoThing!!!!!!!!!!!!!!"
    def execute_select(self, select: ysql.Select):
        all_records = self.__fetech_data(select)

        __product_records = []
        for table, recs in all_records.items():
            __product_records.append([])
            for rec in recs:
                __product_records[-1].append({ table: rec })
        def record_product_to_dict(product: Tuple[Dict]):
            ans = {}
            for factor in product:
                ans.update(factor)
            return ans
        product_records_generator = map(record_product_to_dict, itertools.product(*__product_records, repeat=1))
        
        ret_records = []
        for rec in product_records_generator:
            if self.__where_filter(select.where, rec, select.params):
                ret_rec = {}
                for res_column in select.columns:
                    # literal column result
                    if res_column.type == ysql.Column.COLUMN_TYPE_LITERAL:
                        ret_rec[res_column.name] = res_column.value
                        continue

                    def extract_column_value(column_name: str):
                        tmp = column_name.split(".")
                        table_ref = list(rec.keys())[0]
                        column_ref = tmp[-1]
                        if len(tmp) == 2:
                            table_ref = tmp[0]
                        #SEQ.NEXTVAL
                        if column_name.endswith(".NEXTVAL"):
                            return self.sequences[table_ref].nextval()
                        #TABLE.COLUMN
                        else:
                            return rec[table_ref][column_ref]

                    if res_column.type == ysql.Column.COLUMN_TYPE_FUNCTION:
                        value = None
                        #NVL(XXXX, default)
                        if res_column.value.name == "NVL":
                            value = extract_column_value(res_column.value.params[0])
                            value = value or res_column.value.params[1].value

                        ret_rec[res_column.name] = value
                    else:
                        value = extract_column_value(res_column.value)
                        ret_rec[res_column.name] = value
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
        for table in select.tables:
            if table.name == "DUAL":
                all_records["DUAL"] = [{}]
                continue
            if table.type == ysql.Table.TABLE_TYPE_SUBSELECT:
                all_records[table.name] = self.execute_select(self, table.ent)
            else:
                all_records[table.name] = self.tables[table.ent].get_all_records()
        return all_records
    def __where_filter(self, where: List[ysql.CondGroup], data: Dict, params: Dict[str, str]):
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
        # 提取条件列的值: 因为条件列可能有多种形式，1. 数字 2. 字符串 3. 表字段 4. 参数 
        def extract_cond_value(cond_operand: ysql.Value):
            if cond_operand.type == ysql.Value.VALUE_TYPE_LITERAL:
                return cond_operand.ent
            elif cond_operand.type == ysql.Value.VALUE_TYPE_PARAM:
                return params[cond_operand.ent]
            elif cond_operand.type == ysql.Value.VALUE_TYPE_COLUMN:
                cond_detail = cond_operand.ent.split(".")
                column = cond_detail[-1]
                table = list(data.keys())[0]
                if len(cond_detail) == 2:
                    table = cond_detail[0]
                return data[table][column]

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

    def execute_insert(insert: ysql.Insert) -> int:
        # TODO
        return 0

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
        if not isinstance(self.stmt, ysql.Select):
            return None
        if self.cursor_data is None or self.cursor_pos >= len(self.cursor_data):
            return None
        data = self.cursor_data[self.cursor_pos]
        self.cursor_pos += 1
        return data
    def fetchone_with_order(self):
        data = self.fetchone() or {}
        ret = []
        for col in self.stmt.columns:
            ret.append(data[col.name])
        return ret

DBS: Dict[str, Database] = {}
SESSIONS: Dict[int, Session] = {}