'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 配置文件读取
'''
from configparser import ConfigParser
import os


class Config(object):
    """
    配置文件读取
    """

    def __init__(self, section, conf_path="./config/config.ini"):
        """
        构造函数

        Parameters
        ----------
        section: str. config.ini中的section，即括号中的部分
        conf_path: str. 配置文件位置，默认为'./config/config.ini'

        Returns
        -------
        None.
        """
        self.section = section
        # 判断是否存在该文件
        if os.path.exists(conf_path):
            self.conf_path = conf_path
        else:
            raise FileExistsError("conf_path does not exists!")

    def get_config(self, option):
        """
        Description
        ----------
        获取配置文件信息

        Parameters
        ----------
        option: str. 指定section下的选项，相当于key

        Returns
        -------
        Dict. 配置文件信息
        """
        conf = ConfigParser()
        conf.read(self.conf_path)
        conf_dct = dict(conf.items(self.section))
        return conf_dct[option]
