'''
Author: dkl
Description: 下载股票数据
Date: 2023-03-19 11:08:37
'''
import unittest
import pandas as pd
from download.asharedaily import AshareDailyDownload
from download.tradecal import TradecalDownload
from typing import List
test_date = '20230315'


class TestAShareDaily(unittest.TestCase):

    def test_set_trade_date_lst(self):
        db = AshareDailyDownload()
        data_delete(db, 'asharedailyprices', trade_date=test_date)
        db._set_trade_date_lst('asharedailyprices')
        print(db.trade_date_lst)
        self.assertIsInstance(db.trade_date_lst, List)

    def test_download_daily_prices(self):
        db = AshareDailyDownload()
        data_delete(db, 'asharedailyprices', trade_date=test_date)
        db.download_dailyprices()
        df = data_select(db, 'asharedailyprices', trade_date=test_date)
        print(df)
        self.assertGreater(len(df), 0)

    def test_download_dailybasic(self):
        db = AshareDailyDownload()
        data_delete(db, 'asharedailybasic', trade_date=test_date)
        db.download_dailybasic()
        df = data_select(db, 'asharedailybasic', trade_date=test_date)
        print(df)
        self.assertGreater(len(df), 0)

    def test_download_stockbasic(self):
        db = AshareDailyDownload()
        data_delete(db, 'asharestockbasic', trade_date=None)
        db.download_stockbasic()
        df = data_select(db, 'asharestockbasic', trade_date=None)
        print(df)
        self.assertGreater(len(df), 0)


class TestTradeCal(unittest.TestCase):
    def test_download_trade_cal(self):
        db = TradecalDownload()
        data_delete(db, 'asharetradecal', trade_date=None)
        db.download_main()
        df = data_select(db, 'asharetradecal', trade_date=None)
        print(df)
        self.assertGreater(len(df), 0)


def data_delete(db, table_name, trade_date=None):
    if trade_date is not None:
        sql = f'delete from {table_name} where trade_date={trade_date};'
    else:
        sql = f'delete from {table_name};'
    conn = db.engine.connect()
    conn.execute(sql)
    conn.close()
    return


def data_select(db, table_name, trade_date=None):
    if trade_date is not None:
        sql = f'select * from {table_name} where trade_date={trade_date};'
    else:
        sql = f'select * from {table_name};'
    df = pd.read_sql(sql, con=db.engine)
    return df
