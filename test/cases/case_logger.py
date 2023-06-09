'''
Author: dkl
Description: 日志测试
Date: 2023-04-07 14:14:48
'''
import unittest
import os
import logging
from utils.logger import Logger
path = './log/test.log'


class TestLogger(unittest.TestCase):
    def setUp(self):
        '''
        每次创建前，先检查下有没有test.log文件，有的话删掉
        '''
        if os.path.exists(path):
            os.remove(path)

    def tearDown(self):
        '''
        每次运行完函数都把test.log删除
        '''
        logging.shutdown()
        if os.path.exists(path):
            os.remove(path)

    def test_basiclog(self):
        logger = Logger('test_basic_log', file_dir=path)
        logger.info('This is an info message')
        logger.warning('This is a warning message')
        logger.error('This is an error message')
        logger.critical('This is a critical message')

    def test_send_error_email(self):
        logger = Logger('test_send_error_email', file_dir=path)
        logger.info('This is an info message')
        logger.warning('This is a warning message')
        logger.error('This is an error message')
        logger.critical('This is a critical message')
        logger.send_error_email()
