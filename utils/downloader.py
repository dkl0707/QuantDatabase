'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 下载器
'''
import datetime
from time import sleep

from utils.logger import Logger

logger = Logger("TushareDownloader")


class Downloader(object):
    """
    下载器基类
    """

    def __init__(self, sleeptime, maxreqs, maxtries):
        """
        初始化类

        Parameters
        ----------
        sleeptime: int.休眠时间, 达到最大请求次数后休眠时间.
        maxreqs: int.最大允许请求次数，减小服务器压力.
        maxtries: int.允许连续报错的最大次数.
        """
        # 休眠部分
        self._sleeptime = sleeptime
        # 请求部分
        self._reqcount = 0
        self._maxreqs = maxreqs
        # 报错部分
        self._exceptcount = 0
        self._maxtries = maxtries


class TushareDownloader(Downloader):
    def __init__(self, sleeptime=60, maxreqs=300, maxtries=500):
        """
        初始化类

        Parameters
        ----------
        sleeptime: int.休眠时间, 达到最大请求次数后休眠时间. 默认60s
        maxreqs: int.最大允许请求次数，减小服务器压力.默认300
        maxtries: int.允许连续报错的最大次数，默认为500
        """
        super().__init__(sleeptime, maxreqs, maxtries)

    def download(self, func, *args, **kwargs):
        """
        下载数据, 请求次数不能太多，超过限额就sleep。另外报错的时候也进行sleep

        Parameters
        ----------
        func: 函数. 调取的api接口
        args: 非键值对的可变数量的参数列表
        kwargs: 键值对的可变数量的参数列表

        Returns
        -------
        下载的数据。超过报错次数限额退出下载过程
        """
        # 请求次数超过限额，sleep
        if self._reqcount > self._maxreqs:
            logger.warning("请求次数已满，开始sleep")
            sleep(self._sleeptime)
            self._reqcount = 0
        # 只有在报错次数小于最大允许次数时才会执行
        while self._exceptcount <= self._maxtries:
            start_time = datetime.datetime.now()
            # tushare的实际函数是query, 将名称传入args 
            logger.info(f"开始调用函数pro.{func.args[0]}")
            try:
                # 如果执行成功, 报错次数归0，请求次数+1，跳出循环
                res = func(*args, **kwargs)
                logger.info(f"完成函数pro.{func.args[0]}")
                end_time = datetime.datetime.now()
                cost_time = (end_time - start_time).total_seconds()
                logger.info("共用时: {}s\n".format(cost_time))
                self._reqcount = self._reqcount + 1
                self._exceptcount = 0
                return res
            # 否则就开始打印exception并进行sleep
            # 打印exception的目的在于如果出现函数本身有错误的情况可以及时发现
            except Exception as e:
                logger.warning(e)
                self._exceptcount += 1
                logger.warning("Exception count: %d" % self._exceptcount)
                logger.warning("Force sleep...")
                sleep(self._sleeptime)
                self._reqcount += 1
        # 报错次数超过最大允许次数就报错
        raise TimeoutError("The exception count has reached maxtries")