'''
Author: dkl
Date: 2023-01-14 20:13:54
Description: 日频期货数据下载
'''
import datetime
import pandas as pd
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
from tqdm import tqdm
from sqlalchemy.types import VARCHAR, DECIMAL, INT

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()
# 获取日志记录器
logger = Logger("futdaily")


class FutDailyDownload(DataBase):
    """
    每天的交易数据下载
    """

    def __init__(self, trade_date_lst=None):
        """
        Description
        ----------
        类初始化.

        Parameters
        ----------
        trade_date_lst: List[str]. 默认为None, 即交易日历中有，但表中没有的数据

        Return
        ----------
        None
        """
        super().__init__(database="fut_data")
        self.trade_date_lst = trade_date_lst

    def _set_trade_date_lst(self, table, start_date="19950417"):
        """
        Description
        ----------
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table: 数据库表名

        Return
        ----------
        None
        """
        # 默认下载数据库交易日历表至今缺失的数据
        if self.trade_date_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 获取table的交易日列表
        sql1 = f"""select distinct a.trade_date from {table} a;"""
        trade_date_lst1 = pd.read_sql(sql=sql1, con=self.engine)["trade_date"].tolist()

        trade_date_lst2 = self._get_daily_trade_date_lst(start_date)
        trade_date_set = set(trade_date_lst2) - set(trade_date_lst1)
        self.trade_date_lst = sorted(list(trade_date_set))
        return

    def _get_daily_trade_date_lst(self, start_date="19950417"):
        """
        Description
        ----------
        获取从历史到昨天的月度交易日列表

        Parameters
        ----------
        None.

        Return
        ----------
        List[str]. 日频交易日列表
        """
        # 获取从历史至昨天的交易日列表
        end_time = datetime.datetime.now() - datetime.timedelta(days=1)
        end_date = end_time.strftime(r"%Y%m%d")
        sql = f"""select a.cal_date as trade_date from stk_data.asharetradecal a
                  where a.cal_date>='{start_date}' and a.cal_date <='{end_date}'
                  and a.is_open=1;"""
        trade_cal_df = pd.read_sql(sql=sql, con=self.engine)
        trade_date_lst = trade_cal_df["trade_date"].tolist()
        return trade_date_lst

    def _select_trading_contract(self, df):
        """
        Description
        ----------
        筛选实际进行交易的合约，如'A0001',而不是'AL','A'等主力或者连续合约

        Parameters
        ----------
        df: 数据列表，必须含有fut_code

        Return
        ----------
        pandas.DataFrame. 筛选后的数据
        """

        def _select_code_func(code):
            symbol = code.split(".")[0]
            return (symbol[-4:]).isdigit()

        if "fut_code" not in df.columns:
            raise ValueError("fut_code must be in columns.")
        select_flag = df["fut_code"].apply(_select_code_func)
        df = df.loc[select_flag, :].copy()
        df = df.reset_index(drop=True)
        return df

    @logger_decorator(logger)
    def download_main(self):
        self.download_futbasic()
        self.download_dailyprices()
        self.download_futwsr()
        self.download_futholding()

    @logger_decorator(logger)
    def download_dailyprices(self):
        self._set_trade_date_lst("futdailyprices", "19950417")
        for trade_date in tqdm(self.trade_date_lst):
            # 日频数据
            fields_lst = [
                "trade_date",
                "ts_code",
                "open",
                "high",
                "low",
                "close",
                "settle",
                "pre_close",
                "pre_settle",
                "vol",
                "amount",
                "oi",
                "delv_settle",
            ]
            fields = ",".join(fields_lst)
            df = downloader.download(
                pro.fut_daily, trade_date=trade_date, fields=fields
            )
            df["pct_chg"] = 100 * (df["close"] / df["pre_close"] - 1)
            df = df.rename(columns={"ts_code": "fut_code"})
            # 筛选实际交易的合约,如'A0001.DCF'
            df = self._select_trading_contract(df)
            # 存储
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "fut_code": VARCHAR(255),
                "open": DECIMAL(20, 4),
                "high": DECIMAL(20, 4),
                "low": DECIMAL(20, 4),
                "close": DECIMAL(20, 4),
                "settle": DECIMAL(20, 4),
                "pre_close": DECIMAL(20, 4),
                "pre_settle": DECIMAL(20, 4),
                "pct_chg": DECIMAL(20, 4),
                "vol": DECIMAL(20, 4),
                "amount": DECIMAL(20, 4),
                "oi": DECIMAL(20, 4),
                "delv_settle": DECIMAL(20, 4),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="期货日频数据_" + trade_date,
                table_name="futdailyprices",
                dtype=sql_dtype,
            )
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_futbasic(self):
        # 拉取数据
        fields = [
            "ts_code",
            "exchange",
            "name",
            "multiplier",
            "trade_unit",
            "per_unit",
            "quote_unit",
            "quote_unit_desc",
            "d_mode_desc",
            "list_date",
            "delist_date",
            "d_month",
            "last_ddate",
            "trade_time_desc",
        ]
        df = pd.DataFrame()
        exchange_lst = ["CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"]
        for exchange in exchange_lst:
            tempdf = downloader.download(
                pro.fut_basic, exchange=exchange, fields=fields
            )
            tempdf = tempdf.rename(columns={"ts_code": "fut_code"})
            tempdf = self._select_trading_contract(tempdf)
            tempdf["is_after_hours_trading"] = tempdf["trade_time_desc"].apply(
                lambda x: 1 if "夜盘" in x else 0
            )
            tempdf = tempdf.drop(columns=["trade_time_desc"])
            df = pd.concat([df, tempdf])
        df = df.reset_index(drop=True)
        sql_dtype = {
            "fut_code": VARCHAR(255),
            "exchange": VARCHAR(255),
            "name": VARCHAR(255),
            "multiplier": INT(),
            "trade_unit": VARCHAR(255),
            "per_unit": INT(),
            "quote_unit": VARCHAR(255),
            "quote_unit_desc": VARCHAR(255),
            "d_mode_desc": VARCHAR(255),
            "list_date": VARCHAR(255),
            "delist_date": VARCHAR(255),
            "d_month": VARCHAR(255),
            "last_ddate": VARCHAR(255),
            "is_after_hours_trading": VARCHAR(255),
        }
        df = df[list(sql_dtype.keys())].copy()
        self.store_data(
            data=df,
            data_name="期货合约信息表数据",
            table_name="futbasic",
            flag_replace=True,
            dtype=sql_dtype,
        )
        return

    @logger_decorator(logger)
    def download_futwsr(self):
        self._set_trade_date_lst("futwsr", "20060106")
        # 日度数据下载
        for trade_date in tqdm(self.trade_date_lst):
            # 日频数据
            fields_lst = [
                "trade_date",
                "symbol",
                "exchange",
                "warehouse",
                "vol",
                "pre_vol",
                "area",
                "year",
                "unit",
            ]
            fields = ",".join(fields_lst)
            df = downloader.download(pro.fut_wsr, trade_date=trade_date, fields=fields)
            df = df.drop_duplicates(["trade_date", "symbol", "warehouse"])
            df = df.reset_index(drop=True)
            sql_dtype = {
                "trade_date": VARCHAR(255),
                "symbol": VARCHAR(255),
                "exchange": VARCHAR(255),
                "warehouse": VARCHAR(255),
                "vol": DECIMAL(20, 4),
                "pre_vol": DECIMAL(20, 4),
                "area": VARCHAR(255),
                "year": VARCHAR(255),
                "unit": VARCHAR(255),
            }
            df = df[list(sql_dtype.keys())].copy()
            self.store_data(
                data=df,
                data_name="期货仓单日报_" + trade_date,
                table_name="futwsr",
                dtype=sql_dtype,
            )
        # 将self.trade_date_lst重设为None
        self.trade_date_lst = None
        return

    @logger_decorator(logger)
    def download_futholding(self):
        exchange_lst = ["CFFEX", "CZCE", "DCE", "SHFE"]
        start_date_dct = {
            "CFFEX": "20100416",
            "CZCE": "20050429",
            "DCE": "20060104",
            "SHFE": "20020107",
        }
        for exchange in exchange_lst:
            db_name = "futholding" + exchange.lower()
            start_date = start_date_dct[exchange]
            self._set_trade_date_lst(db_name, start_date)
            for trade_date in tqdm(self.trade_date_lst):
                # 日频数据
                fields_lst = [
                    "trade_date",
                    "symbol",
                    "broker",
                    "vol",
                    "long_hld",
                    "short_hld",
                ]
                fields = ",".join(fields_lst)
                df = downloader.download(
                    pro.fut_holding,
                    trade_date=trade_date,
                    exchange=exchange,
                    fields=fields,
                )
                sql_dtype = {
                    "trade_date": VARCHAR(255),
                    "symbol": VARCHAR(255),
                    "broker": VARCHAR(255),
                    "vol": DECIMAL(20, 4),
                    "long_hld": DECIMAL(20, 4),
                    "short_hld": DECIMAL(20, 4),
                }
                df = df[list(sql_dtype.keys())].copy()
                data_name = "期货每日持仓" + exchange + "_" + trade_date
                self.store_data(
                    data=df, data_name=data_name, table_name=db_name, dtype=sql_dtype
                )
                # 将self.trade_date_lst重设为None
            self.trade_date_lst = None
        return
