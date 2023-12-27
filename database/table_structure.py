'''
Author: dkl
Date: 2023-01-23 22:49:27
Description: 拉取表结构
'''
from database.database import DataBase
import os
import datetime
import pandas as pd
from utils.conf import Config
from utils.logger import logger_decorator, Logger

# 获取日志记录器
logger = Logger("table_structure")


class TableStructure(DataBase):
    def __init__(self):
        super().__init__("information_schema")
        # 获取配置文件路径
        conf = Config("table_structure")
        self.table_structure_path = conf.get_config("table_structure_path")
        self.table_index_path = conf.get_config("table_index_path")
        self.table_comment_path = conf.get_config("table_comment_path")
        # 数据库字符串
        conf_db_str = conf.get_config("database_lst")
        conf_db_str = conf_db_str.replace(" ", "")
        db_lst = conf_db_str.split(',')
        db_lst = ['"' + i + '"' for i in db_lst]
        self.db_string = ",".join(db_lst)

    @logger_decorator(logger)
    def pull_table_structure(self):
        """
        拉取表结构
        """
        # 依次为数据库名，表名，字段名，字段顺序，是否为空，字段类型，字段是否为主键, 字段注释
        sql = f"""select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION,
                  IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY, COLUMN_COMMENT
                  from COLUMNS
                  where TABLE_SCHEMA in ({self.db_string});"""
        df = pd.read_sql(sql=sql, con=self.engine)
        df.to_csv(self.table_structure_path, index=False)
        return pd.read_csv(self.table_structure_path)

    @logger_decorator(logger)
    def pull_table_index(self):
        """
        拉取表索引
        """
        # 依次为数据库名，表名，索引名，字段名，字段顺序
        sql = f"""select TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE,
                  INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, INDEX_TYPE
                  from STATISTICS
                  where TABLE_SCHEMA in ({self.db_string});"""
        df = pd.read_sql(sql=sql, con=self.engine)
        df.to_csv(self.table_index_path, index=False)
        return pd.read_csv(self.table_index_path)

    @logger_decorator(logger)
    def pull_table_comment(self):
        """
        拉取表注释
        """
        # 依次为数据库名，表名，索引名，字段名，字段顺序
        sql = f"""select TABLE_SCHEMA, TABLE_NAME, TABLE_COMMENT
                  from TABLES
                  where TABLE_SCHEMA in ({self.db_string});"""
        df = pd.read_sql(sql=sql, con=self.engine)
        df.to_csv(self.table_comment_path, index=False)
        return pd.read_csv(self.table_comment_path)

    @logger_decorator(logger)
    def pull_all_structure(self, rename_old_table=True, clear_past_days=7):
        """
        从服务器数据库中拉取表格

        Parameters
        ----------
        rename_old_table: bool. 是否将已有表格存储为历史文件. 默认为True
        clear_past_days: int. 需要去除多少天前的表格结构. 默认为7.
            如果输入为0，则不去除历史表格结构

        Returns
        -------
        tuple(pandas.DataFrame). 表结构, 索引结构, 表注释
        """
        # 如果建立新的表格，对现在的表格加上日期
        if rename_old_table:
            try:
                self._rename_old_structure_df()
            except Exception as e:
                raise FileExistsError(e)
        # 删除clear_past_days之前的表格
        if clear_past_days > 0:
            try:
                self._clear_history_structure_df(clear_past_days=clear_past_days)
            except Exception as e:
                raise FileExistsError(e)
        tb_struct_df = self.pull_table_structure()
        tb_ind_df = self.pull_table_index()
        tb_comm_df = self.pull_table_comment()
        return tb_struct_df, tb_ind_df, tb_comm_df

    @logger_decorator(logger)
    def _rename_old_structure_df(self):
        """
        根据已有的表结构文件路径，重命名已有的表结构文件
        如果表结构文件不存在会发出警告，但不会报错
        """
        path_lst = [
            self.table_structure_path,
            self.table_index_path,
            self.table_comment_path,
        ]
        for path in path_lst:
            if not os.path.exists(path):
                logger.warning(f"{path} 路径不存在,跳过该路径")
                continue
            # 在文件名后面加入date,重新存储
            filename = os.path.basename(path)
            filedir = os.path.dirname(path)
            today_date_str = datetime.datetime.today().strftime(r"%Y%m%d")
            new_file_name = filename[:-4] + "_" + today_date_str + filename[-4:]
            new_path = os.path.join(filedir, new_file_name)
            df = pd.read_csv(path)
            df.to_csv(new_path, index=False)
        return

    @logger_decorator(logger)
    def _clear_history_structure_df(self, clear_past_days=7):
        """
        删除历史clear_past_days之前的表结构

        Parameters
        ----------
        clear_past_days: int. 多少天之前需要删除的表结构
        """
        today_datetime = datetime.datetime.today()
        dir_path = "./table_structure/"
        # 现在的文件所在位置
        now_path_lst = [
            self.table_structure_path,
            self.table_index_path,
            self.table_comment_path,
        ]
        # 现在的文件名
        now_name_lst = [os.path.basename(path) for path in now_path_lst]
        for filename in os.listdir(dir_path):
            # 现在的文件跳过不删
            if filename in now_name_lst:
                continue
            file_datetime = datetime.datetime.strptime(filename[-12:-4], r"%Y%m%d")
            if (today_datetime - file_datetime).days >= clear_past_days:
                os.remove(dir_path + filename)
        return
