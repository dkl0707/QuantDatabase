'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 基础爬虫类
'''
from fake_useragent import UserAgent
import threading
import requests
from time import sleep
from utils.logger import Logger


# 获取日志记录器
logger = Logger('basicspyder')


class BasicSpyder(object):
    """
    基本爬虫框架. 封装爬取单个url的方法
    """

    def __init__(self, maxtries=20, timeout=5, sleeptime=30):
        """
        构造函数

        Parameters
        ----------
        maxtries: int. 最大重试次数, 默认为20次
        timeout: int. 超时时间, 默认为5秒
        sleeptime: int. 超时/爬取失败后的休眠时间. 默认为30秒
        """
        self.maxtries = maxtries
        self.timeout = timeout
        self.sleeptime = sleeptime
        self.lock = threading.Lock()

    def get(self, url, params=None):
        """
        获取指定爬取网址的response

        Parameters
        ----------
        url: str. 指定爬取网址
        params: dict. 爬取网址输入参数。默认为None.

        Returns
        -------
        requests.response
        """
        if not isinstance(url, str):
            raise ValueError('url must be str')
        for retries in range(1, self.maxtries + 1):
            try:
                headers = {'User-Agent': UserAgent().random}
                response = requests.get(
                    url=url,
                    params=params,
                    timeout=self.timeout,
                    headers=headers
                )
                if response.status_code == 200:
                    return response
                else:
                    logger.warning(f'status_code={response.status_code}')
                    logger.warning(f'response data: {response.text}')
                    sleep(self.sleeptime)
                    continue
            except Exception as e:
                logger.warning(e)
                logger.warning(f'超时{retries}次, 进行sleep')
        raise TimeoutError('超时次数过多，退出程序')
