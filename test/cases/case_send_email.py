'''
Author: dkl
Description: 测试发送邮件
Date: 2023-03-19 11:08:37
'''
import unittest
from utils.sendemail import SendEmail


class TestSendEmail(unittest.TestCase):

    def test_send_email(self):
        se = SendEmail()
        subject = '服务器发送邮件测试'
        body = '<p>县长来了，鹅城太平了!<p>'
        se.send_email(subject=subject, body=body)
