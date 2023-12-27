'''
Author: dkl
Date: 2023-03-19 10:50:36
Description: 测试主函数
'''
import unittest
from BeautifulReport import BeautifulReport
from utils.logger import SendEmail


def test_all_cases(report_html=True):
    """
    对所有样例进行测试

    Parameters
    ----------
    report_html : Bool, optional
        是否以html形式展现, 默认为True
    """
    # 加载目录下所有用例模块
    case_path = "./test/cases"
    # start_dir是用例模块的路径，pattern是模块名
    discover = unittest.defaultTestLoader.discover(start_dir=case_path,
                                                   pattern="case*.py")
    if not report_html:
        # 实例化一个运行器
        runner = unittest.TextTestRunner()
        runner.run(discover)
    else:
        br = BeautifulReport(discover)
        # 文件名filename，文件路径report_dir，文件描述description
        br.report(filename='test_all_result.html',
                  description="测试报告",
                  report_dir='./test')
        # 发送测试报告邮件
        se = SendEmail()
        subject = '代码测试报告'
        body = '<p>这是代码的测试报告，请查收!<p>'
        attach = {
            'file': './test/test_all_result.html',
            'file_name': 'test_all_result.html'
        }
        se.send_email(subject=subject, body=body, attach=attach)


def test_simple_case():
    """
    少数测试用例测试
    """
    # 实例化测试套件
    suite01 = unittest.TestSuite()
    # 将测试用例加入测试套件. 按照加入的顺序运行
    from test.cases.case_logger import TestLogger
    suite01.addTest(TestLogger("test_send_error_email"))
    # 实例化一个运行器
    runner = unittest.TextTestRunner()
    # 运行suite01这个测试套件
    runner.run(suite01)