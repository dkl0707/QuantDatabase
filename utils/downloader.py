from time import sleep
from utils.logger import Logger, logger_decorator
logger1 = Logger('TushareDownloader')
logger2 = Logger('SpyderDownloader')


class Downloader(object):
    """
    下载器基类
    """
    def __init__(self, sleeptime, maxreqs, maxtries):
        '''
        Description
        ----------
        初始化类

        Parameters
        ----------
        sleeptime: int.休眠时间, 达到最大请求次数后休眠时间.
        maxreqs: int.最大允许请求次数，减小服务器压力.
        maxtries: int.允许连续报错的最大次数.

        Return
        ----------
        None
        '''
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
        '''
        Description
        ----------
        初始化类

        Parameters
        ----------
        sleeptime: int.休眠时间, 达到最大请求次数后休眠时间. 默认60s
        maxreqs: int.最大允许请求次数，减小服务器压力.默认300
        maxtries: int.允许连续报错的最大次数，默认为500

        Return
        ----------
        None
        '''
        super().__init__(sleeptime, maxreqs, maxtries)

    @logger_decorator(logger1)
    def download(self, func, *args, **kwargs):
        '''
        Description
        ----------
        下载数据, 请求次数不能太多，超过限额就sleep。另外报错的时候也进行sleep

        Parameters
        ----------
        func: 函数. 调取的api接口
        args: 非键值对的可变数量的参数列表
        kwargs: 键值对的可变数量的参数列表

        Return
        ----------
        下载的数据。超过报错次数限额退出下载过程
        '''
        # 请求次数超过限额，sleep
        if self._reqcount > self._maxreqs:
            logger1.warning('请求次数已满，开始sleep')
            sleep(self._sleeptime)
            self._reqcount = 0
        # 只有在报错次数小于最大允许次数时才会执行
        while self._exceptcount <= self._maxtries:
            try:
                # 如果执行成功, 报错次数归0，请求次数+1，跳出循环。不用进行日志记录
                res = func(*args, **kwargs)
                self._reqcount = self._reqcount + 1
                self._exceptcount = 0
                return res
            # 否则就开始打印exception并进行sleep
            # 打印exception的目的在于如果出现函数本身有错误的情况可以及时发现
            except Exception as e:
                logger1.warning(e)
                self._exceptcount += 1
                logger1.warning("Exception count: %d" % self._exceptcount)
                logger1.warning("Force sleep...")
                sleep(self._sleeptime)
                self._reqcount += 1
        # 报错次数超过最大允许次数就报错
        raise TimeoutError('The exception count has reached maxtries')


class SpyderDownloader(Downloader):
    """
    用于爬虫下载数据
    """
    def __init__(self, sleeptime=60, maxreqs=50, maxtries=20):
        '''
        Description
        ----------
        初始化类

        Parameters
        ----------
        sleeptime: int.休眠时间, 达到最大请求次数后休眠时间. 默认60s
        maxreqs: int.最大允许请求次数，减小服务器压力.默认50
        maxtries: int.允许连续报错的最大次数，默认为20

        Return
        ----------
        None
        '''
        super().__init__(sleeptime, maxreqs, maxtries)

    @logger_decorator(logger2)
    def download(self, func, *args, **kwargs):
        '''
        Description
        ----------
        下载数据, 请求次数不能太多，超过限额就sleep。另外报错的时候也进行sleep

        Parameters
        ----------
        func: 函数. 调取的api接口
        args: 非键值对的可变数量的参数列表
        kwargs: 键值对的可变数量的参数列表
        Return
        ----------
        下载的数据。超过报错次数限额退出下载过程
        '''
        # 请求次数超过限额，sleep
        if self._reqcount > self._maxreqs:
            logger2.warning('请求次数已满，开始sleep')
            sleep(self._sleeptime)
            self._reqcount = 0
        # 只有在报错次数小于最大允许次数时才会执行
        while self._exceptcount <= self._maxtries:
            try:
                # 如果执行成功, 报错次数归0，请求次数+1，跳出循环。不用进行日志记录
                res = func(*args, **kwargs)
                self._reqcount = self._reqcount + 1
                self._exceptcount = 0
                return res
            # 否则就开始打印exception并进行sleep
            # 打印exception的目的在于如果出现函数本身有错误的情况可以及时发现
            except Exception as e:
                logger2.warning(e)
                self._exceptcount += 1
                logger2.warning("Exception count: %d" % self._exceptcount)
                logger2.warning("Force sleep...")
                sleep(self._sleeptime)
                self._reqcount += 1
        # 报错次数超过最大允许次数就报错
        raise TimeoutError('The exception count has reached maxtries')
