'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 日志
'''
import logging
import datetime
from functools import wraps
from utils.sendemail import SendEmail

today_date = datetime.datetime.today().strftime(r"%Y%m%d")


class BasicLogger(object):
    """
    基本日志类
    """

    def __init__(self, name, file_dir=None):
        """
        基本日志类的构造函数

        Parameters
        ----------
        name: str. 日志器名称
        log_file: str. 文件位置
        """
        # 设定文件日志器
        format_str = "[%(asctime)s] %(name)s - %(levelname)s: %(message)s"
        self.formatter = logging.Formatter(format_str)
        # 设定logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        if file_dir is None:
            # 在log文件夹下创建文件
            self.file_dir = "./log/" + today_date + ".log"
        else:
            self.file_dir = file_dir
        self.file_name = self.file_dir.split("/")[-1]
        # 屏幕输出
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(logging.DEBUG)
        self.console_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.console_handler)
        # 文件输出
        self.file_handler = logging.FileHandler(self.file_dir, encoding="utf-8")
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)


class Logger(BasicLogger, SendEmail):
    """
    带有发送邮件功能的日志类
    """

    def __init__(self, name, file_dir=None):
        BasicLogger.__init__(self, name, file_dir)
        SendEmail.__init__(self)
        self.email_subject = "你数据库炸了-{date}".format(date=today_date)

    def send_error_email(self):
        html = "数据库炸了，请查看以下日志!"
        attach = {
            "file": self.file_dir,
            "file_name": self.file_name,
        }
        self.send_email(subject=self.email_subject, body=html, attach=attach)


def logger_decorator(logger):
    def decorator(func):
        @wraps(func)
        def inner_wrapper(*args, **kwargs):
            logger.info(f"开始调用函数{func.__name__}")
            start_time = datetime.datetime.now()
            try:
                result = func(*args, **kwargs)
            except Exception as err_desc:
                logger.error(f"函数{func.__name__}出了问题，具体报错如下:")
                logger.error(err_desc)
            else:
                logger.info(f"完成函数{func.__name__}")
                end_time = datetime.datetime.now()
                cost_time = (end_time - start_time).total_seconds()
                logger.info("共用时: {}s\n".format(cost_time))
                return result

        return inner_wrapper

    return decorator
