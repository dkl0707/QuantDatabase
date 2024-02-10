'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 日频数据下载
'''
import datetime
import pandas as pd
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
from typing import List
from tqdm import tqdm
from sqlalchemy.types import VARCHAR, DECIMAL

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()
# 获取日志记录器
logger = Logger("asharedaily")


class AshareDailyDownload(DataBase):
    """
    每天的交易数据下载
    """

    def __init__(self, trade_date_lst: List[str] = None):
        """
        类初始化.

        Parameters
        ----------
        trade_date_lst: List[str]. 默认为None, 即交易日历中有，但表中没有的数据
        """
        super().__init__(database="stk_data")
        self.trade_date_lst = trade_date_lst

    def _set_trade_date_lst(self, table_name):
        """
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table_name: 数据库表名
        """
        # 默认下载数据库交易日历表至今缺失的数据
        if self.trade_date_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 获取table_name的交易日列表
        sql1 = f"""select distinct a.trade_date from {table_name} a;"""
        trade_date_lst1 = pd.read_sql(sql=sql1, con=self.engine)["trade_date"].tolist()

        trade_date_lst2 = self._get_daily_trade_date_lst()
        trade_date_set = set(trade_date_lst2) - set(trade_date_lst1)
        self.trade_date_lst = sorted(list(trade_date_set))
        return

    def _get_daily_trade_date_lst(self):
        """
        获取从历史到昨天的日频交易日列表

        Parameters
        ----------
        None.

        Returns
        -------
        List[str]. 日频交易日列表
        """
        # 获取从历史至昨天的交易日列表
        last_date = datetime.datetime.now() - datetime.timedelta(days=1)
        last_dt = last_date.strftime(r"%Y%m%d")
        sql = f"""select a.cal_date as trade_date from asharetradecal a
                  where a.cal_date<='{last_dt}' and a.is_open=1;"""
        trade_cal_df = pd.read_sql(sql=sql, con=self.engine)
        trade_date_lst = trade_cal_df["trade_date"].tolist()
        return trade_date_lst

    @logger_decorator(logger)
    def download_main(self):
        self.download_stockbasic()
        self.download_dailyprices()
        self.download_dailybasic()

    @logger_decorator(logger)
    def download_dailyprices(self):
        self._set_trade_date_lst("asharedailyprices")
        # 日频数据下载
        for trade_date in tqdm(self.trade_date_lst):
            # 日频数据
            fields_lst = [
                "trade_date",
                "ts_code",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "vol",
                "amount",
            ]
            fields = ",".join(fields_lst)
            df1 = downloader.download(pro.daily, trade_date=trade_date, fields=fields)
            df1["pct_chg"] = 100 * (df1["close"] / df1["pre_close"] - 1)
            # 复权因子
            df2 = downloader.download(pro.adj_factor, trade_date=trade_date)
            df = pd.merge(df1, df2, on=["trade_date", "ts_code"])
            df = df.rename(columns={"ts_code": "stock_code"})
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "stock_code": VARCHAR(255),
                "open": DECIMAL(20, 4),
                "high": DECIMAL(20, 4),
                "low": DECIMAL(20, 4),
                "close": DECIMAL(20, 4),
                "pre_close": DECIMAL(20, 4),
                "pct_chg": DECIMAL(20, 4),
                "vol": DECIMAL(20, 4),
                "amount": DECIMAL(20, 4),
                "adj_factor": DECIMAL(20, 4),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="股票日频数据_" + trade_date,
                table_name="asharedailyprices",
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_dailybasic(self):
        self._set_trade_date_lst("asharedailybasic")
        # 日度数据下载
        for trade_date in tqdm(self.trade_date_lst):
            # 日频数据
            fields_lst = [
                "trade_date",
                "ts_code",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "dv_ratio",
                "dv_ttm",
                "total_share",
                "float_share",
                "free_share",
                "total_mv",
                "circ_mv",
            ]
            fields = ",".join(fields_lst)
            df = downloader.download(
                pro.daily_basic, trade_date=trade_date, fields=fields
            )
            df = df.rename(columns={"ts_code": "stock_code"})
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "stock_code": VARCHAR(255),
                "turnover_rate": DECIMAL(20, 4),
                "turnover_rate_f": DECIMAL(20, 4),
                "volume_ratio": DECIMAL(20, 4),
                "pe": DECIMAL(20, 4),
                "pe_ttm": DECIMAL(20, 4),
                "pb": DECIMAL(20, 4),
                "ps": DECIMAL(20, 4),
                "ps_ttm": DECIMAL(20, 4),
                "dv_ratio": DECIMAL(20, 4),
                "dv_ttm": DECIMAL(20, 4),
                "total_share": DECIMAL(20, 4),
                "float_share": DECIMAL(20, 4),
                "free_share": DECIMAL(20, 4),
                "total_mv": DECIMAL(20, 4),
                "circ_mv": DECIMAL(20, 4),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="股票日频指标_" + trade_date,
                table_name="asharedailybasic",
                dtype=sql_dtype,
            )
        # 将self.trade_date_lst重设为None
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_stockbasic(self):
        # 拉取数据
        fields = [
            "ts_code",
            "name",
            "area",
            "market",
            "exchange",
            "list_date",
            "delist_date",
        ]
        df = pd.DataFrame()
        for list_status in ["L", "D", "P"]:
            tempdf = downloader.download(
                pro.stock_basic, list_status=list_status, fields=fields
            )
            df = pd.concat([df, tempdf])
        df = df.reset_index(drop=True)
        df = df.rename(columns={"ts_code": "stock_code"})
        sql_dtype = {
            "stock_code": VARCHAR(255),
            "name": VARCHAR(255),
            "area": VARCHAR(255),
            "market": VARCHAR(255),
            "exchange": VARCHAR(255),
            "list_date": VARCHAR(255),
            "delist_date": VARCHAR(255),
        }
        df = df[list(sql_dtype.keys())].copy()
        self.store_data(
            data=df,
            data_name="股票基本情况表数据",
            table_name="asharestockbasic",
            flag_replace=True,
            dtype=sql_dtype,
        )
        return
