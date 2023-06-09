'''
Author: dkl
Description: 申万指数爬取
Date: 2023-05-02 11:07:03
'''
import pandas as pd
import requests
from utils.logger import Logger, logger_decorator
from time import sleep

# 获取日志记录器
logger = Logger('spyder_swindex')


@logger_decorator(logger)
def sw_hist_daily(index_code, timeout=60, max_retries=10, sleeptime=30):
    '''
    Description
    ----------
    下载申万指数历史日频数据
    申万宏源研究-指数发布-指数详情-指数历史数据
    地址: https://www.swhyresearch.com/institute-sw/api/index_publish/trend/

    Parameters
    ----------
    index_code: str. 申万行业指数代码, 如'801010.SI'
    timeout: int. 超时时间，默认为10秒
    max_retries: int. 爬取最大重试次数，默认为10次
    sleeptime: int. 睡眠时间, 默认为10秒

    Return
    ----------
    pandas.DataFrame. 申万指数历史日频数据
    '''
    url = 'https://www.swhyresearch.com/institute-sw/api/index_publish/trend/'
    params = {
        'swindexcode': index_code[0:-3],
        'period': 'DAY',
    }
    for retries in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            data_json = r.json()
        except Exception as e:
            if retries == max_retries:
                raise TimeoutError('超时次数过多，退出程序')
            logger.warning(e)
            logger.warning('超时%f次, 进行sleep' % retries)
            sleep(sleeptime)
        else:
            df = pd.DataFrame(data_json['data'])
            continue
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
    df = df[[
        'trade_date',
        'index_code',
        'open',
        'high',
        'low',
        'close',
    ]]
    df['index_code'] = index_code
    df['trade_date'] = df['trade_date'].apply(lambda x: ''.join(x.split('-')))
    # 涨跌幅(保留到后四位)
    df['pct_chg'] = 100 * (df['close'] / df['close'].shift(1) - 1)
    df['pct_chg'] = df['pct_chg'].apply(lambda x: round(x, 4))
    df.loc[0, 'pct_chg'] = 0
    return df
