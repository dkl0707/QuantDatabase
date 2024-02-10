'''
Author: dkl
Description: 测试读取表和创建表
Date: 2023-03-19 11:08:37
'''
import unittest
import pandas as pd
from database.database import DataBase
from database.table_structure import TableStructure


class TestDataBase(unittest.TestCase):

    def setUp(self):
        # 每次执行前，都先创建一个表，然后download到本地
        sql = """CREATE TABLE ccass_hold_detail_2077(
                 trade_date VARCHAR(255) NOT NULL COMMENT '交易日期',
                 stock_code VARCHAR(255) NOT NULL COMMENT '股票代码',
                 col_participant_id VARCHAR(255) NOT NULL COMMENT '机构编号',
                 col_participant_name VARCHAR(255) NOT NULL COMMENT '机构名称',
                 col_shareholding BIGINT NOT NULL COMMENT  '持股数量',
                 PRIMARY KEY (trade_date, stock_code, col_participant_id),
                 UNIQUE KEY id_date_code_ind(col_participant_id asc, trade_date asc, stock_code asc) USING BTREE
                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment='2077年沪深港通机构持股';"""
        db = DataBase('stk_data')
        conn = db.engine.connect()
        conn.execute(sql)
        conn.close()
        ts = TableStructure()
        ts.pull_all_structure()

    def tearDown(self):
        # 执行后，删除table,重新下载table_structure
        sql = "drop table ccass_hold_detail_2077;"
        db = DataBase('stk_data')
        conn = db.engine.connect()
        conn.execute(sql)
        conn.close()
        ts = TableStructure()
        ts.pull_all_structure()

    def test_read_create_table_struct(self):
        db = DataBase('stk_data')
        create_table_struct = db._read_create_table_struct('ccass_hold_detail_2077')
        for key, value in create_table_struct.items():
            print(key, ':')
            print(value)

    def test_get_create_table_sql(self):
        db = DataBase('stk_data')
        sql = db._check_table_exists('ccass_hold_detail_2077')
        print(sql)

    def test_check_table_exists(self):
        db = DataBase('stk_data')
        flag = db._check_table_exists('ccass_hold_detail_2077')
        self.assertEqual(flag, True)

    def test_store_data(self):
        data = pd.DataFrame({
            'trade_date': ['20770201', '20770301', '20771030'],
            'stock_code': ['666666.SH', '233333.SZ', '777777.BJ'],
            'col_participant_id': ['A1', 'A2', 'A3'],
            'col_participant_name': ['中信', '中金', '中建投'],
            'col_shareholding': [1, 3, 4],
        })
        db = DataBase('stk_data')
        db.store_data(data, 'test_data', 'ccass_hold_detail_2077')
        output_df = pd.read_sql('select * from ccass_hold_detail_2077', con=db.engine)
        print(output_df)
        self.assertEqual(len(output_df), 3)
