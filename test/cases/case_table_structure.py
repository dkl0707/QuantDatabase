'''
Author: dkl
Description: 测试读取表和创建表
Date: 2023-03-19 11:08:37
'''
import unittest
from database.table_structure import TableStructure


class TestTableStructure(unittest.TestCase):

    def test_pull_all_structure(self):
        ts = TableStructure()
        tb_struct_df, tb_ind_df, tb_comm_df = ts.pull_all_structure()
        # 打印三个df
        print('struct_df:\n', tb_struct_df)
        print('ind_df:\n', tb_ind_df)
        print('comm_df:\n', tb_comm_df)
        # 判断dataframe长度是否为0，并打印
        flag_struct = (len(tb_struct_df) != 0)
        flag_ind = (len(tb_ind_df) != 0)
        flag_comm = (len(tb_comm_df) != 0)
        flag = flag_struct & flag_ind & flag_comm
        self.assertEqual(flag, True)
