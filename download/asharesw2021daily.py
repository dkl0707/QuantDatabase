'''
Author: dkl
Date: 2023-08-14 20:13:54
Description: 申万行业指数(2021年版)日频数据下载
'''
import pandas as pd
import os
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from spyder.swindex import SWDataSpyder
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
from utils.utils import divide_lst
import datetime
from sqlalchemy.types import VARCHAR, DECIMAL, SMALLINT
import threading

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()

# 获取日志记录器
logger = Logger("asharesw2021daily")


class AshareSW2021DailyDownload(DataBase):
    """
    申万2021行业指数数据下载
    """

    def __init__(self, trade_date_lst=None):
        """
        类初始化.

        Parameters
        ----------
        trade_date_lst: List[str]. 默认为None, 即交易日历中有，但表中没有的数据
        ind_code_lst: List[str]. 默认为None, 即全部的行业代码
        """
        super().__init__(database="stk_data")
        self.trade_date_lst = trade_date_lst
        self.lock = threading.Lock()

    def _get_daily_trade_date_lst(self):
        """
        获取从历史到昨天的日频交易日列表

        Parameters
        ----------
        None.

        Returns
        -------
        List[str]. 月度交易日列表
        """
        # 获取从历史至昨天的交易日列表
        last_date = datetime.datetime.now() - datetime.timedelta(days=1)
        last_dt = last_date.strftime(r"%Y%m%d")
        sql = f"""select a.cal_date as trade_date from asharetradecal a
                  where a.cal_date<='{last_dt}' and a.is_open=1;"""
        trade_cal_df = pd.read_sql(sql=sql, con=self.engine)
        trade_date_lst = trade_cal_df["trade_date"].tolist()
        return trade_date_lst

    def _set_trade_date_lst(self, table_name):
        '''
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table_name: 数据库表名
        '''
        # 默认下载数据库交易日历表至今缺失的数据
        if self.trade_date_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 获取table_name的交易日列表
        sql1 = f"""select distinct a.trade_date from {table_name} a;"""
        trade_date_df1 = pd.read_sql(sql=sql1, con=self.engine)
        trade_date_lst1 = trade_date_df1["trade_date"].tolist()
        trade_date_lst2 = self._get_daily_trade_date_lst()
        trade_date_set = set(trade_date_lst2) - set(trade_date_lst1)
        self.trade_date_lst = sorted(list(trade_date_set))
        return

    @logger_decorator(logger)
    def download_main(self):
        self.download_indexbasic()
        self.download_member()
        self.download_dailyprices()

    def _check_indexbasic(self):
        sql = 'select count(*) from asharesw2021basic;'
        if self.execute_sql(sql)[0][0]!=31:
            return False
        return True

    @logger_decorator(logger)
    def download_indexbasic(self):
        if self._check_indexbasic():
           return
        index_basic = pro.index_classify(src='SW2021', level='L1')
        index_basic = index_basic[['index_code', 'industry_name']].copy()
        index_basic.columns = ["index_code", "name"]
        sql_dtype = {
            "index_code": VARCHAR(255),
            "name": VARCHAR(255),
        }
        self.store_data(
            data=index_basic,
            data_name="申万行业指数(2021年版)基本情况表数据",
            table_name="asharesw2021basic",
            flag_replace=True,
            dtype=sql_dtype,
        )
        return

    @logger_decorator(logger)
    def download_member(self):
        # 拉取数据
        fields = ["index_code", "con_code", "in_date", "out_date", "is_new"]
        index_code_df = pro.index_classify(src='SW2021', level='L1')
        index_code_lst = index_code_df["index_code"].tolist()
        df = pd.DataFrame()
        for index_code in index_code_lst:
            tempdf = downloader.download(
                pro.index_member, index_code=index_code, fields=fields
            )
            tempdf['is_new'] = tempdf['is_new'].replace({'Y':1, 'N':0})
            tempdf = tempdf.sort_values(["con_code", "in_date"])
            df = pd.concat([df, tempdf])
        df = df.reset_index(drop=True)
        sql_dtype = {
            "index_code": VARCHAR(255),
            "con_code": VARCHAR(255),
            "in_date": VARCHAR(255),
            "out_date": VARCHAR(255),
            "is_new": SMALLINT,
        }
        df = df[list(sql_dtype.keys())].copy()
        self.store_data(
            data=df,
            data_name="申万行业指数(2021年版)成分股数据",
            table_name="asharesw2021member",
            flag_replace=True,
            dtype=sql_dtype,
        )
        return

    @logger_decorator(logger)
    def download_dailyprices(self, n_threads=10):
        self._set_trade_date_lst("asharesw2021daily")
        # 获取申万行业指数列表
        ind_code_df = pro.index_classify(src="SW2021", level="L1")
        ind_code_lst = ind_code_df["index_code"].tolist()
        ind_code_lst_thread = divide_lst(ind_code_lst, n_threads)
        spyder = SWDataSpyder()

        def spyder_main(ind_code_lst):
            nonlocal spyder, self
            for code in ind_code_lst:
                tempdf = spyder.sw_daily(code, self.trade_date_lst)
                self.lock.acquire()
                if os.path.exists('./tmp/sw_daily.csv'):
                    bigdf = pd.read_csv('./tmp/sw_daily.csv')
                else:
                    bigdf = pd.DataFrame()
                if tempdf is not None:
                    bigdf = pd.concat([bigdf, tempdf])
                    bigdf.to_csv('./tmp/sw_daily.csv', index=False)
                self.lock.release()
                logger.info(f'SW2021DAILY Finished code: {code}')
        try:
            # 创建线程并启动
            threads = []
            for ind_code_lst in ind_code_lst_thread:
                thread = threading.Thread(target=spyder_main,
                                          kwargs={'ind_code_lst': ind_code_lst})
                threads.append(thread)
                thread.start()
            # 等待所有线程完成
            for thread in threads:
                thread.join()

            df = pd.read_csv('./tmp/sw_daily.csv')
            df = df.drop_duplicates()
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "index_code": VARCHAR(255),
                "open": DECIMAL(20, 4),
                "high": DECIMAL(20, 4),
                "low": DECIMAL(20, 4),
                "close": DECIMAL(20, 4),
                "pct_chg": DECIMAL(20, 4),
            }
            self.store_data(
                data=df,
                data_name="申万行业指数(2021年版)日频数据",
                table_name="asharesw2021daily",
                dtype=sql_dtype,
            )
        except Exception as e:
            logger.error(e)
        if os.path.exists('./tmp/sw_daily.csv'):
            os.remove('./tmp/sw_daily.csv')
        self.trade_date_lst = None
        return
