'''
Author: dkl
Description: 申万指数爬取
Date: 2023-05-02 11:07:03
'''
import pandas as pd
from utils.basicspyder import BasicSpyder
from utils.logger import Logger, logger_decorator
from typing import List


# 获取日志记录器
logger = Logger('swindex_spyder')


class SWDataSpyder(BasicSpyder):
    @logger_decorator(logger)
    def sw_daily(self, index_code: str, trade_date_lst: List[str] = None):
        """
        下载申万指数历史日频数据
        申万宏源研究-指数发布-指数详情-指数历史数据
        地址: https://www.swsresearch.com/

        Parameters
        ----------
        index_code: str
            申万行业指数代码, 如'801010.SI'
        trade_date_lst: List[str], optional
            指定交易日期列表, 默认为None，即取全部数据

        Returns
        -------
        pandas.DataFrame.
            申万指数历史日频数据
        """
        url = 'https://www.swsresearch.com/institute-sw/api/index_publish/trend/'
        params = {
            'swindexcode': index_code[0:-3],
            'period': 'DAY'
        }
        response = self.get(url, params)
        data_json = response.json()
        df = pd.DataFrame(data_json['data'])
        df.rename(
            columns={
                'swindexcode': 'index_code',
                'bargaindate': 'trade_date',
                'openindex': 'open',
                'maxindex': 'high',
                'minindex': 'low',
                'closeindex': 'close',
                'hike': '',
                'markup': '',
                'bargainamount': '',
                'bargainsum': '',
            },
            inplace=True,
        )
        df = df[['trade_date', 'index_code', 'open', 'high', 'low', 'close']]
        df['index_code'] = index_code
        df['trade_date'] = df['trade_date'].apply(lambda x: x.replace('-', ''))
        if trade_date_lst is not None:
            df = df.loc[df['trade_date'].isin(trade_date_lst), :].copy()
        df = df.reset_index(drop=True)
        # 涨跌幅(保留到后四位)
        df['pct_chg'] = 100 * (df['close'] / df['close'].shift(1) - 1)
        df['pct_chg'] = df['pct_chg'].apply(lambda x: round(x, 4))
        if (len(df) != 0):
            df.loc[0, 'pct_chg'] = 0
        return df