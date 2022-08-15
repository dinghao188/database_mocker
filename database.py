import database_mocker.ysql as ysql
import itertools
from typing import Any, List,Dict,Tuple,Union

__all__ = ["Table", "Database", "Session", "DBS", "SESSIONS"]

class Table:
    def __init__(self, name: str, columns: Tuple[str], records: List[Tuple] = None):
        self.name = name
        self.columns = columns
        self.records = records or []
    def clear(self):
        self.records = []
    def insert_record(self, data: Dict[str, Union[str,int]]):
        '''
        insert a record into  this table
            record's key is column
            record's value is column value
            the columns which specified will be None
        '''
        record = []
        for col in self.columns:
            if col in data.keys():
                record.append(data[col])
            else:
                record.append(None)
        self.records.append(record)
    def insert_records(self, data: List[Tuple]):
        '''
        insert some records into this table, you must specify all value of columns by order
        '''
        self.records.extend(data)
    def get_all_records(self):
        res = []
        for record in self.records:
            res.append(dict(zip(self.columns, record)))
        return res
    def get_record(self, data: Dict[str, Union[str, int, float]]):
        '''
        按存放结构返回一条和data各字段都吻合的数据
        '''
        for rec in self.records:
            match = True
            for i, col in enumerate(self.columns):
                if col in data.keys() and data[col] is not None and data[col] != rec[i]:
                    match = False
                    break
            if match:
                return rec
        return None

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
        # 获取所有表的数据
        # {"table1": [data,]}
        all_records = self.__fetech_data(select)

        # 所有数据加上表名
        # [[table1_data,], [table2_data,], [table3_data,]]
        # tableX_data = {"tableX": data}
        __product_records = []
        for table, recs in all_records.items():
            __product_records.append([])
            for rec in recs:
                __product_records[-1].append({ table: rec })
                
        # 不同表数据之间进行笛卡尔组合，最终得到的记录形式为
        # {table1: dataX, table2: dataX, table3: dataX,}
        def record_product_to_dict(product: Tuple[Dict]):
            ans = {}
            for factor in product:
                ans.update(factor)
            return ans
        product_records_generator = map(record_product_to_dict, itertools.product(*__product_records, repeat=1))
        
        # 对得到的记录进行过滤
        ret_records = []
        for rec in product_records_generator:
            if self.__where_filter(select.where, rec, select.params):
                ret_rec = self.__generate_result(select, rec, select.params)
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
    def __extract_value(self, value: ysql.Value, full_column_data: Dict[str, Dict], sql_params: Dict[str, Union[str, int]]):
        if value.type == ysql.Value.VALUE_TYPE_LITERAL:
            return value.ent
        elif value.type == ysql.Value.VALUE_TYPE_PARAM:
            return sql_params[value.ent]
        elif value.type == ysql.Value.VALUE_TYPE_COLUMN:
            tmp = value.ent.split('.')
            value_column = tmp[-1]
            value_table = list(full_column_data.keys())[0]
            if len(tmp) == 2:
                value_table = tmp[0]
            # 序列
            if value.ent.endswith('.NEXTVAL'):
                return self.sequences[value_table].nextval()
            # 表数据
            else:
                return full_column_data[value_table][value_column]
        elif value.type == ysql.Value.VALUE_TYPE_FUNCTION:
            ret_value = None
            #NVL(XXXX, default)
            if value.ent.name == "NVL":
                ret_value = self.__extract_value(value.ent.params[0], full_column_data, sql_params)
                ret_value = ret_value or self.__extract_value(value.ent.params[1], full_column_data, sql_params)
            return ret_value
        else:
            return None
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

        for cond_group in where:
            match = True
            for cond in cond_group.conds:
                if cond.cond_groups is not None:
                    match = match and self.__where_filter(cond.cond_groups, data)
                else:
                    left_value = self.__extract_value(cond.left, data, params)
                    right_value = self.__extract_value(cond.right, data, params)
                    match = match and cmp_funcs[cond.op](left_value, right_value)
                if not match:
                    break
            if match:
                return True
        return False
    def __generate_result(self, select: ysql.Select, full_column_data: Dict[str, Dict], sql_params: Dict[str, Any]) -> Dict[str, Union[str, int]]:
        '''
        根据SQL中的结果列信息，构造一条返回数据
        '''
        # 根据SQL中的结果列信息，构造返回数据
        ret_rec = {}
        for res_column in select.columns:
            ret_rec[res_column.name] = self.__extract_value(res_column.value, full_column_data, sql_params)
        return ret_rec
    def execute_insert(self, insert: ysql.Insert) -> int:
        # TODO
        return 0

    def __getitem__(self, table_name) -> Table:
        return self.tables[table_name]

class Session:
    def __init__(self, session_id, db_name):
        self.session_id = session_id
        self.db_name = db_name
        self.clear()
    def clear(self):
        self.stmt = None
        self.clear_cursor()
    def clear_cursor(self):
        self.cursor_data = None
        self.cursor_pos = -1
    def fetchone(self) -> Dict[str, Union[str, int, float]]:
        if not isinstance(self.stmt, ysql.Select):
            return None
        self.cursor_pos += 1
        if self.cursor_data is None or self.cursor_pos >= len(self.cursor_data):
            return None
        data = self.cursor_data[self.cursor_pos]
        return data
    def fetch_cur_rawdata(self) -> Tuple[Union[str, int, float]]:
        '''
        获取当前游标下的数据，返回结果是对应表的按顺序的列值列表
        '''
        if not isinstance(self.stmt, ysql.Select):
            return None
        if self.cursor_data is None or self.cursor_pos >= len(self.cursor_data):
            return None
        data = self.cursor_data[self.cursor_pos]
        data = DBS[self.db_name][self.stmt.tables[0].ent].get_record(data) or ()
        return data

DBS: Dict[str, Database] = {}
SESSIONS: Dict[int, Session] = {}