'''
Author: dkl
Date: 2023-01-14 20:13:54
Description: 月频数据下载
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
logger = Logger("asharemonthly")


class AshareMonthlyDownload(DataBase):
    """
    每月的交易数据下载
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

    def _set_trade_date_lst(self, table):
        """
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table: 数据库表名
        """
        # 默认下载数据库交易日历表至今缺失的数据
        if self.trade_date_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 获取table的交易日列表
        sql = f"""select distinct a.trade_date from {table} a;"""
        trade_date_lst1 = pd.read_sql(sql=sql, con=self.engine)["trade_date"].tolist()

        # 获取从历史至昨天的交易日列表
        trade_date_lst2 = self._get_monthly_trade_date_lst()
        trade_date_set = set(trade_date_lst2) - set(trade_date_lst1)
        self.trade_date_lst = sorted(list(trade_date_set))
        return

    def _get_monthly_trade_date_lst(self):
        """
        获取从历史到昨天的月频交易日列表

        Parameters
        ----------
        None.

        Return
        ----------
        List[str]. 月频交易日列表
        """
        # 获取从历史至上个月最后一天的交易日列表
        # 获取上个月最后一天
        today_date = datetime.datetime.now()
        this_month_first_date = datetime.datetime(
            year=today_date.year, month=today_date.month, day=1
        )
        last_date = this_month_first_date - datetime.timedelta(days=1)
        last_dt = last_date.strftime(r"%Y%m%d")

        # 获取历史至上个月最后一天的交易日
        sql = f"""select a.cal_date as trade_date from asharetradecal a
                  where str_to_date(a.cal_date,'%%Y%%m%%d')
                  <=str_to_date('{last_dt}','%%Y%%m%%d')
                  and a.is_open=1;"""
        trade_cal_df = pd.read_sql(sql=sql, con=self.engine)

        # 输出年和月，根据年和月筛出每年每月最后一天
        trade_cal_df = trade_cal_df.sort_values("trade_date")
        trade_cal_df = trade_cal_df.reset_index(drop=True)
        trade_cal_df["ym"] = trade_cal_df["trade_date"].apply(lambda x: x[0:6])
        trade_cal_df = trade_cal_df.drop_duplicates(subset=["ym"], keep="last")
        trade_date_lst = trade_cal_df["trade_date"].tolist()
        return trade_date_lst

    @logger_decorator(logger)
    def download_main(self):
        self.download_monthlyprices()

    @logger_decorator(logger)
    def download_monthlyprices(self):
        self._set_trade_date_lst("asharemonthlyprices")
        # 月频数据下载
        for trade_date in tqdm(self.trade_date_lst):
            # 月频数据
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
            df1 = downloader.download(pro.monthly, trade_date=trade_date, fields=fields)
            # 复权因子
            df2 = downloader.download(pro.adj_factor, trade_date=trade_date)
            df = pd.merge(df1, df2, on=["trade_date", "ts_code"])
            df = df.rename(columns={"ts_code": "stock_code"})
            # 计算涨跌幅
            df["pct_chg"] = 100 * (df["close"] / df["pre_close"] - 1)
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
                data_name="月频数据_" + trade_date,
                table_name="asharemonthlyprices",
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return
