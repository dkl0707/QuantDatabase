'''
Author: dkl
Date: 2023-08-14 20:13:54
Description: 指数数据下载
'''
import pandas as pd
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
import datetime
from tqdm import tqdm
from sqlalchemy.types import VARCHAR, DECIMAL

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()

# 获取日志记录器
logger = Logger("ashareindex")
# 需要下载的指数列表
index_basic_dct = {
    '000001.SH': '上证综指',
    '399001.SZ': '深证成指',
    '399006.SZ': '创业板指',
    '899050.BJ': '北证50',
    '000688.SH': '科创50',
    '000698.SH': '科创100',
    '000016.SH': '上证50',
    '399850.SZ': '深证50',
    '399330.SZ': '深证100',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
    '000906.SH': '中证800',
    '000852.SH': '中证1000',
    '932000.CSI': '中证2000',
    '399311.SZ': '国证1000',
    '399303.SZ': '国证2000',
}


class AshareIndexDownload(DataBase):
    """
    指数数据下载
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

    def _set_trade_date_lst(self, table_name, date_type='daily'):
        '''
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table_name: str. 数据库表名
        date_type: str. 日期格式，分为daily和monthly. 默认为"daily"
        '''
        # 默认下载数据库交易日历表至今缺失的数据
        if self.trade_date_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 获取table_name的交易日列表
        sql1 = f"""select distinct a.trade_date from {table_name} a;"""
        trade_date_df1 = pd.read_sql(sql=sql1, con=self.engine)
        trade_date_lst1 = trade_date_df1["trade_date"].tolist()
        if date_type == 'daily':
            trade_date_lst2 = self._get_daily_trade_date_lst()
        elif date_type == 'monthly':
            trade_date_lst2 = self._get_monthly_trade_date_lst()
        else:
            raise ValueError('date_type must be daily or monthly.')
        trade_date_set = set(trade_date_lst2) - set(trade_date_lst1)
        self.trade_date_lst = sorted(list(trade_date_set))
        return

    @logger_decorator(logger)
    def download_main(self):
        self.download_indexbasic()
        self.download_daily()
        self.download_monthly()
        self.download_weight()

    def _check_indexbasic(self):
        sql = 'select count(*) from asharesw2021basic;'
        if self.execute_sql(sql)[0][0] != len(list(index_basic_dct.keys())):
            return False
        return True

    @logger_decorator(logger)
    def download_indexbasic(self):
        if self._check_indexbasic():
            return
        index_basic = pd.DataFrame({
            'index_code': list(index_basic_dct.keys()),
            'name': list(index_basic_dct.values())
        })
        sql_dtype = {
            "index_code": VARCHAR(255),
            "name": VARCHAR(255),
        }
        self.store_data(
            data=index_basic,
            data_name="指数基本情况表数据",
            table_name="ashareindexbasic",
            flag_replace=True,
            dtype=sql_dtype,
        )
        return

    @logger_decorator(logger)
    def download_daily(self):
        self._set_trade_date_lst(table_name='ashareindexdaily',
                                 date_type='daily')
        if len(self.trade_date_lst) == 0:
            self.trade_date_lst = None
            return
        start_date = self.trade_date_lst[0]
        end_date = self.trade_date_lst[-1]
        # 日频数据下载
        for index_code in tqdm(list(index_basic_dct.keys())):
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
            df = downloader.download(pro.index_daily,
                                     ts_code=index_code,
                                     start_date=start_date,
                                     end_date=end_date,
                                     fields=fields)
            df.loc[df['pre_close']<1e-2, 'pre_close'] = df['close']
            df["pct_chg"] = 100 * (df["close"] / df["pre_close"] - 1)
            df = df.loc[df['trade_date'].isin(self.trade_date_lst), :].copy()
            df = df.rename(columns={'ts_code': 'index_code'})
            df = df.reset_index(drop=True)
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "index_code": VARCHAR(255),
                "open": DECIMAL(20, 4),
                "high": DECIMAL(20, 4),
                "low": DECIMAL(20, 4),
                "close": DECIMAL(20, 4),
                "pre_close": DECIMAL(20, 4),
                "pct_chg": DECIMAL(20, 4),
                "vol": DECIMAL(30, 4),
                "amount": DECIMAL(30, 4),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="指数日频数据_" + index_code,
                table_name="ashareindexdaily",
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_monthly(self):
        self._set_trade_date_lst(table_name='ashareindexmonthly',
                                 date_type='monthly')
        if len(self.trade_date_lst) == 0:
            self.trade_date_lst = None
            return
        start_date = self.trade_date_lst[0]
        end_date = self.trade_date_lst[-1]
        # 月频数据下载
        for index_code in tqdm(list(index_basic_dct.keys())):
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
            df = downloader.download(pro.index_monthly,
                                     ts_code=index_code,
                                     start_date=start_date,
                                     end_date=end_date,
                                     fields=fields)
            df.loc[df['pre_close']<1e-2, 'pre_close'] = df['close']
            df["pct_chg"] = 100 * (df["close"] / df["pre_close"] - 1)
            df = df.loc[df['trade_date'].isin(self.trade_date_lst), :].copy()
            df = df.rename(columns={'ts_code': 'index_code'})
            df = df.reset_index(drop=True)
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "index_code": VARCHAR(255),
                "open": DECIMAL(20, 4),
                "high": DECIMAL(20, 4),
                "low": DECIMAL(20, 4),
                "close": DECIMAL(20, 4),
                "pre_close": DECIMAL(20, 4),
                "pct_chg": DECIMAL(20, 4),
                "vol": DECIMAL(30, 4),
                "amount": DECIMAL(30, 4),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="指数月频数据_" + index_code,
                table_name="ashareindexmonthly",
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_weight(self):
        self._set_trade_date_lst(table_name='ashareindexweight',
                                 date_type='monthly')
        # 拉取数据
        df = pd.DataFrame()
        fields = ["index_code", "con_code", "trade_date", "weight"]
        # 起始日期
        start_date_dct = {
            '000001.SH': '19901219',
            '399001.SZ': '19940720',
            '399006.SZ': '20100531',
            '899050.BJ': '20220429',
            '000688.SH': '20191231',
            '000698.SH': '20191231',
            '000016.SH': '20031231',
            '399850.SZ': '20021231',
            '399330.SZ': '20021231',
            '000300.SH': '20041231',
            '000905.SH': '20041231',
            '000906.SH': '20041231',
            '000852.SH': '20041231',
            '932000.CSI': '20131231',
            '399311.SZ': '20021231',
            '399303.SZ': '20091231',
        }
        for index_code in tqdm(list(index_basic_dct.keys())):
            start_date = start_date_dct[index_code] 
            for trade_date in self.trade_date_lst:
                if trade_date < start_date:
                    continue
                tempdf = downloader.download(
                    pro.index_weight, trade_date=trade_date,
                    index_code=index_code, fields=fields
                )
                logger.info(f'存储指数{index_code}成分股权重数据, 日期{trade_date}')
                df = pd.concat([df, tempdf])
            df = df.sort_values(["con_code", "trade_date"])
            df = df.reset_index(drop=True)
            sql_dtype = {
                "index_code": VARCHAR(255),
                "con_code": VARCHAR(255),
                "trade_date": VARCHAR(255),
                "weight": DECIMAL(20, 4)
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="指数成分股权重数据_" + index_code,
                table_name="ashareindexweight",
                flag_replace=True,
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return
