'''
Author: dkl
Date: 2023-01-14 20:13:54
Description: 股票日期下载
'''
import datetime
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from utils.logger import Logger, logger_decorator
from utils.downloader import TushareDownloader
from sqlalchemy.types import VARCHAR, INT

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()
# 获取日志记录器
logger = Logger("tradecal")


class TradecalDownload(DataBase):
    def __init__(self):
        super().__init__("stk_data")

    def _check_flag_download(self):
        """
        检查数据库中交易日历的最后一天是否距离昨天不到30天

        Returns
        -------
        Bool. 数据库中交易日历的最后一天是否距离昨天不到30天
        """
        logger.info("检查数据库中交易日历的最后一天是否距离昨天不到30天...")
        # 读取数据库中交易日历的最后一天
        sql = "select cal_date from asharetradecal order by cal_date desc limit 1;"
        conn = self.engine.connect()
        res = conn.execute(sql).fetchall()
        if len(res) == 0:
            conn.close()
            return True
        else:
            db_last_date = datetime.datetime.strptime(res[0][0], r"%Y%m%d")
            conn.close()
            # 昨天
            yester_date = datetime.datetime.now() - datetime.timedelta(days=1)
            # 检查数据库最后一天离昨天是否在30天内
            flag = db_last_date - yester_date < datetime.timedelta(days=30)
            logger.info("检查完成!")
            return flag

    @logger_decorator(logger)
    def download_main(self):
        download_flag = self._check_flag_download()
        if download_flag:
            logger.info("数据库最后一天离今天在30天内，下载交易日历！")
            fields_lst = ["cal_date", "is_open"]
            fields = ",".join(fields_lst)
            trade_cal_df = downloader.download(
                pro.trade_cal,
                start_date="19491001",
                end_date="20991231",
                fields=fields,
                exchange="SSE",
            )
            # 转化成datetime格式，排序，重置索引后，存入数据库
            trade_cal_df = trade_cal_df.sort_values("cal_date", ascending=True)
            trade_cal_df = trade_cal_df.reset_index(drop=True)
            sql_dtype = {"cal_date": VARCHAR(255), "is_open": INT()}
            trade_cal_df = trade_cal_df[list(sql_dtype.keys())].copy()
            # 存储数据
            self.store_data(
                data=trade_cal_df,
                data_name="交易日历",
                table_name="asharetradecal",
                flag_replace=True,
                dtype=sql_dtype,
            )
            return
        logger.info("数据库最后一个交易日距离当下仍有30天以上，无需下载交易日历！")
        return
