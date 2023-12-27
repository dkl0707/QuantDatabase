'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 工具性函数
'''
import pandas as pd
from typing import List


def get_hist_nearest_date(target_date: str, trade_date_lst: List[str]) -> str:
    """
    在交易日列表中找到离所选日期最近的历史日期

    Parameters
    ----------
    target_date : str
        所选日期
    trade_date_lst: List[str]
        交易日列表

    Returns
    -------
    str
        交易日列表中离所选日期最近的历史日期
    """

    dt_lst = [pd.to_datetime(trade_dt) for trade_dt in trade_date_lst]
    target_dt = pd.to_datetime(target_date)
    if target_dt < dt_lst[0]:
        raise ValueError("target_dt早于trade_date_lst第一个日期!")
    for i in range(0, len(dt_lst) - 1):
        last_dt = dt_lst[i]
        next_dt = dt_lst[i + 1]
        if (last_dt <= target_dt) and (target_dt < next_dt):
            res_date = last_dt.strftime(r"%Y%m%d")
            return res_date
    res_date = dt_lst[-1].strftime(r"%Y%m%d")
    return res_date


def divide_lst(lst: List[str], n_groups: int) -> List[List]:
    """
    将指定列表lst分成n_groups组
    若最后一组数量不足int(len(lst)/n_groups)，则包含剩余数据

    Parameters
    ----------
    lst : _type_
        _description_
    n_groups : _type_
        _description_

    Returns
    -------
    List[List]
        分组结果
    """
    # 每个组应该分配的数量
    group_num = int(len(lst)/n_groups)
    res_lst = []
    for i in range(n_groups-1):
        idx_start = group_num * i
        idx_end = group_num * (i + 1)
        res_lst.append(lst[idx_start: idx_end])
    res_lst.append(lst[group_num*(n_groups-1):])
    return res_lst
