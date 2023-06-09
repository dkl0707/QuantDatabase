'''
Author: dkl
Description: 申万行业指数(2021年版)日频数据下载
Date: 2023-01-14 20:13:54
'''
import pandas as pd
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from spyder.swindex import sw_hist_daily
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
from tqdm import tqdm
from sqlalchemy.types import VARCHAR, DECIMAL

# 获取token
tstoken = Config('tushare').get_config('tstoken')
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()

# 获取日志记录器
logger = Logger('asharesw2021daily')


class AshareSW2021DailyDownload(DataBase):
    '''
    每天的交易数据下载
    '''

    def __init__(self, trade_date_lst=None):
        '''
        Description
        ----------
        类初始化.

        Parameters
        ----------
        trade_date_lst: List[str]. 默认为None, 即交易日历中有，但表中没有的数据
        ind_code_lst: List[str]. 默认为None, 即全部的行业代码

        Return
        ----------

        '''
        super().__init__(database='stk_data')
        self.trade_date_lst = trade_date_lst

    @logger_decorator(logger)
    def download_main(self):
        self.download_dailyprices()

    @logger_decorator(logger)
    def download_dailyprices(self):
        # 获取申万行业指数列表
        ind_code_df = pro.index_classify(src='SW2021', level='L1')
        ind_code_lst = ind_code_df['index_code'].tolist()
        # 删除数据库数据
        self.clear_table('asharesw2021daily')
        df = pd.DataFrame()
        for ind_code in tqdm(ind_code_lst):
            df = sw_hist_daily(ind_code)
            sql_dtype = {
                'trade_date': VARCHAR(255),
                'index_code': VARCHAR(255),
                'open': DECIMAL(20, 4),
                'high': DECIMAL(20, 4),
                'low': DECIMAL(20, 4),
                'close': DECIMAL(20, 4),
                'pct_chg': DECIMAL(20, 4)
            }
            data_name = '申万行业指数(2021年版)日频数据_' + ind_code
            self.store_data(data=df,
                            data_name=data_name,
                            table_name='asharesw2021daily',
                            dtype=sql_dtype)
        return
