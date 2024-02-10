'''
Author: dkl
Date: 2022-10-09 23:24:58
Descripttion: 数据库操作
'''
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from utils.conf import Config
from utils.downloader import TushareDownloader
from utils.logger import Logger, logger_decorator

# 获取downloader
downloader = TushareDownloader()
# 获取日志记录器
logger = Logger("database")
# 数据库连接模板
template_url = (
    "mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}?charset=UTF8MB4"
)


class DataBase(object):
    @logger_decorator(logger)
    def __init__(self, database):
        """
        调用相应的数据库

        Parameters
        -------
        database : str. 数据库名字.
        """
        # mysql配置信息
        mysql_conf = Config("mysql")
        user = mysql_conf.get_config("user")
        passwd = mysql_conf.get_config("password")
        host = mysql_conf.get_config("host")
        port = mysql_conf.get_config("port")
        self.database = database
        # 表结构文件设置为空
        self.table_struct_df = None
        self.table_ind_df = None
        self.table_comment_df = None

        # 连接数据库
        engine_url = template_url.format(
            user=user, passwd=passwd, host=host, port=port, database=self.database
        )
        engine = create_engine(engine_url)
        try:
            conn = engine.connect()
            conn.execute("select 1")
            conn.close()
        except ConnectionError as e:
            raise ConnectionError(e)
        else:
            self.engine = engine

    @logger_decorator(logger)
    def create_database(self, database_name):
        conn = self.engine.connect()
        if self._check_database_exists(database_name):
            logger.info(f"数据库{database_name}已经存在!")
            return False
        conn.execute(f"create database {database_name};")
        logger.info(f"数据库{database_name}创建成功!")
        return True

    def _check_database_exists(self, database_name):
        conn = self.engine.connect()
        lst = conn.execute("show databases;").fetchall()
        lst = [element[0] for element in lst]
        conn.close()
        return database_name in lst

    @logger_decorator(logger)
    def create_table(self, table_name):
        if self._check_table_exists(table_name):
            logger.info(f"数据库{self.database}中表格{table_name}已经存在!")
            return False
        self._read_local_table_struct_df()
        logger.info(f"数据库{self.database}中表格{table_name}不存在，开始创建")
        # Step1: 读取数据库表结构
        create_table_struct = self._read_create_table_struct(table_name)
        tb_df = create_table_struct["tb_df"]
        ind_df = create_table_struct["ind_df"]
        tb_comm = create_table_struct["tb_comm"]
        # Step2: 创建表的SQL
        tb_sql = self._get_create_table_sql(table_name, tb_df, ind_df, tb_comm)
        # Step3: SQL连接,执行sql语句
        conn = self.engine.connect()
        try:
            conn.execute(tb_sql)
        except Exception as e:
            err_string = "网络连接中断，或是SQL语句可能存在错误, 语句如下:" + tb_sql
            err_string = err_string + "报错内容如下:\n" + e
            raise Exception(err_string)
        conn.close()
        logger.info(f"数据库{self.database}中表格{table_name}创建成功!")
        return True

    def _read_local_table_struct_df(self):
        if not self._check_table_struct_df():
            return
        # table_structure配置信息读取
        table_conf = Config("table_structure")
        table_structure_path = table_conf.get_config("table_structure_path")
        table_index_path = table_conf.get_config("table_index_path")
        table_comment_path = table_conf.get_config("table_comment_path")
        # 读取csv
        self.table_struct_df = pd.read_csv(table_structure_path)
        self.table_ind_df = pd.read_csv(table_index_path)
        self.table_comment_df = pd.read_csv(table_comment_path)

    def _check_table_struct_df(self):
        flag1 = self.table_struct_df is None
        flag2 = self.table_ind_df is None
        flag3 = self.table_comment_df is None
        flag = flag1 | flag2 | flag3
        return flag

    def _check_table_exists(self, table_name):
        conn = self.engine.connect()
        lst = conn.execute("show tables;").fetchall()
        lst = [element[0] for element in lst]
        conn.close()
        return table_name in lst

    def _read_create_table_struct(self, table_name):
        # 表格结构
        tb_df = self.table_struct_df.copy()
        tb_df = tb_df.loc[
            (tb_df["TABLE_SCHEMA"] == self.database)
            & (tb_df["TABLE_NAME"] == table_name),
            :,
        ].copy()
        tb_df = tb_df.sort_values("ORDINAL_POSITION")
        tb_df = tb_df.reset_index(drop=True)
        # 检查数据中是否存在table_name
        if len(tb_df) == 0:
            raise ValueError(f"本地存入表格中没有{table_name}.")
        # 索引
        ind_df = self.table_ind_df.copy()
        ind_df = ind_df.loc[
            (ind_df["TABLE_SCHEMA"] == self.database)
            & (ind_df["TABLE_NAME"] == table_name),
            :,
        ].copy()
        ind_df = ind_df.sort_values(["INDEX_NAME", "SEQ_IN_INDEX"])
        ind_df = ind_df.reset_index(drop=True)
        # 表注释
        tb_comm_df = self.table_comment_df.copy()
        tb_comm = tb_comm_df.loc[
            (tb_comm_df["TABLE_SCHEMA"] == self.database)
            & (tb_comm_df["TABLE_NAME"] == table_name),
            :,
        ]
        tb_comm = tb_comm["TABLE_COMMENT"].values[0]
        tb_comm = "%%".join(tb_comm.split("%"))
        result = {"tb_df": tb_df, "ind_df": ind_df, "tb_comm": tb_comm}
        return result

    def _get_create_table_sql(self, table_name, tb_df, ind_df, tb_comm):
        tb_sql = f"CREATE TABLE {table_name} (\n"
        for i in range(len(tb_df.index)):
            # 字段名
            col_name = tb_df.loc[i, "COLUMN_NAME"]
            # 字段类型
            col_type = tb_df.loc[i, "COLUMN_TYPE"]
            # 字段是否为空
            col_null = "NOT NULL"
            if tb_df.loc[i, "IS_NULLABLE"] == "YES":
                col_null = ""
            # 字段注释
            col_comment = tb_df.loc[i, "COLUMN_COMMENT"]
            if col_comment is np.nan:
                col_comment = ""
            else:
                col_comment = "%%".join(col_comment.split("%"))
                col_comment = " COMMENT " + "'" + col_comment + "'"
            col_string = " ".join([col_name, col_type, col_null, col_comment, ",\n"])
            tb_sql = tb_sql + col_string
        # 加入主键约束和索引
        for temp_ind_name, temp_ind_df in ind_df.groupby("INDEX_NAME"):
            temp_col_lst = temp_ind_df["COLUMN_NAME"].tolist()
            temp_ind_type = temp_ind_df["INDEX_TYPE"].values[0]
            temp_non_unique = int(temp_ind_df["NON_UNIQUE"].values[0])
            string2 = "(" + (", ".join(temp_col_lst)) + ") "
            if temp_ind_name == "PRIMARY":
                string1 = "PRIMARY KEY "
                string3 = ""
            elif temp_non_unique == 0:
                string1 = "UNIQUE KEY " + temp_ind_name + " "
                string3 = "USING " + temp_ind_type
            else:
                string1 = "KEY " + temp_ind_name + " "
                string3 = "USING " + temp_ind_type
            string4 = ",\n"
            string = string1 + string2 + string3 + string4
            tb_sql = tb_sql + string
        # 需要把末尾的',\n'换成'\n)'否则会报错
        tb_sql = tb_sql[:-2] + "\n)"
        comm_string = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        if tb_comm is not np.nan:
            comm_string = comm_string + f" COMMENT='{tb_comm}'"
        tb_sql = tb_sql + comm_string + ";"
        return tb_sql

    @logger_decorator(logger)
    def store_data(
        self, data, data_name, table_name, dtype=None, retries=5, flag_replace=False
    ):
        """
        将数据存入数据库中的某个数据表

        Parameters
        ----------
        data : pd.DataFrame. 存入的数据
        data_name : str. 数据名称
        table_name : str. 要存入的数据表名称.
        dtype: dict. 指定存入sql的数据类型. {columns_name: sql_type}
        retries: int.重试次数，默认为5
        """
        if (len(data) == 0) or (data is None):
            logger.warning('数据为空, 取消存储')
            return
        # 慎重使用pandas.DataFrame.to_sql中的if_exists=replace
        # 因为pandas的to_sql代码里没有rollback，执行失败就直接把表删了TAT
        # 我们使用engine.begin()作为一个上下文管理器，它相当于包装了个事务，可以回滚~~
        for i in range(retries):
            try:
                with self.engine.begin() as conn:
                    if flag_replace:
                        conn.execute(f"delete from {table_name};")
                    data.to_sql(
                        name=table_name,
                        con=conn,
                        index=False,
                        if_exists="append",
                        dtype=dtype,
                    )
                logger.info(data_name + "已经存入" + table_name + "!")
                return
            except Exception as e:
                logger.warning(e)
                logger.warning(data_name + "数据存储失败，重试%d次" % (i + 1))
        logger.error("数据存储失败，重试结束")

    @logger_decorator(logger)
    def clear_table(self, table_name, retries=5):
        for i in range(retries):
            try:
                conn = self.engine.connect()
                conn.execute(f"delete from {table_name};")
                conn.close()
                return
            except Exception as e:
                logger.warning(e)
                logger.warning(table_name + "清除数据失败，重试%d次" % (i + 1))
        logger.error("删除失败，重试结束")

    @logger_decorator(logger)
    def execute_sql(self, sql, retries=5):
        """
        执行指定的sql语句

        Parameters
        ----------
        sql: str. 要执行的sql语句
        retries: int. 重试次数
        """
        for i in range(retries):
            try:
                conn = self.engine.connect()
                res = conn.execute(sql)
                conn.close()
                # 如果没有返回结果就跳过
                if not res.returns_rows:
                    return
                else:
                    return res.fetchall()
            except Exception as e:
                logger.warning(e)
                logger.warning(f"sql语句{sql}执行失败，重试%d次" % (i + 1))
        logger.error("执行失败，重试结束")

    @logger_decorator(logger)
    def rename_table(self, table_name, new_table_name, retries=5):
        sql = f'RENAME TABLE {table_name} TO {new_table_name};'
        self.execute_sql(sql, retries)