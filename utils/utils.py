'''
Author: dkl
Descripttion: 工具性函数
Date: 2022-10-09 23:24:57
'''
import pandas as pd


def get_hist_nearest_date(target_date, trade_date_lst):
    '''
    Description
    ----------
    在交易日列表中找到离所选日期最近的历史日期

    Parameters
    ----------
    target_date: str. 所选日期
    trade_date_lst: List[str]. 交易日列表

    Return
    ----------
    交易日列表中离所选日期最近的历史日期.
    '''
    dt_lst = [pd.to_datetime(trade_dt) for trade_dt in trade_date_lst]
    target_dt = pd.to_datetime(target_date)
    if target_dt < dt_lst[0]:
        raise ValueError('target_dt早于trade_date_lst第一个日期!')
    for i in range(0, len(dt_lst)-1):
        last_dt = dt_lst[i]
        next_dt = dt_lst[i+1]
        if (last_dt <= target_dt) and (target_dt < next_dt):
            res_date = last_dt.strftime(r'%Y%m%d')
            return res_date
    res_date = dt_lst[-1].strftime(r'%Y%m%d')
    return res_date
