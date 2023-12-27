'''
Author: dkl
Date: 2023-12-19 15:56:40
Description: 创建数据库和数据表进行初始化
'''
import pandas as pd
from database.database import DataBase
from utils.conf import Config


def initialize_main():
    """
    创建数据库和数据表进行初始化
    """
    conf = Config('table_structure')
    table_comment_path = conf.get_config('table_comment_path')
    df = pd.read_csv(table_comment_path)
    database_name_lst = df['TABLE_SCHEMA'].tolist()
    table_name_lst = df['TABLE_NAME'].tolist()
    # 数据库创建
    db_mysql = DataBase('mysql')
    for db_name in list(set(database_name_lst)):
        db_mysql.create_database(db_name)
    # 数据表创建
    for db_name, table_name in zip(database_name_lst, table_name_lst):
        db = DataBase(db_name)
        db.create_table(table_name)
    return