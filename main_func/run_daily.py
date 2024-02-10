'''
Author: dkl
Date: 2023-12-19 16:10:06
Description: 每天运行的函数
'''
import os
import datetime
from download import (
    tradecal,
    asharedaily,
    asharemonthly,
    asharefinance,
    asharesw2021daily,
    ashareindex,
    futdaily
)
from utils.logger import Logger
from utils.conf import Config


def clear_past_log_main():
    """
    清除过去几天的日志，清除天数由config.ini的clean_past_log_days决定
    """
    try:
        conf = Config('log')
        remove_past_days = int(conf.get_config('clear_past_log_days'))
        log_dir = './log/'
        for file_name in os.listdir(log_dir):
            flag1 = ((file_name[-4:] == '.log') & (len(file_name) == 12))
            flag2 = (file_name[:-4] >= '19901219')
            flag3 = (file_name[:-4] <= '20991231')
            flag_daily_log = (flag1 & flag2 & flag3)
            if flag_daily_log:
                log_date = file_name[:-4]
                log_datetime = datetime.datetime.strptime(log_date, r'%Y%m%d')
                today_datetime = datetime.datetime.today()
                if (today_datetime - log_datetime).days >= remove_past_days:
                    os.remove(log_dir + file_name)
    except Exception as e:
        raise ValueError('清除日志失败，请检查log文件夹是否在当前项目下!\n' + e)


def download_main():
    """
    下载主函数
    """
    # 交易日历
    asharetradecal_dl = tradecal.TradecalDownload()
    asharetradecal_dl.download_main()
    # A股日频
    asharedaily_dl = asharedaily.AshareDailyDownload()
    asharedaily_dl.download_main()
    # A股月频
    asharemonthly_dl = asharemonthly.AshareMonthlyDownload()
    asharemonthly_dl.download_main()
    # 指数
    ashareindex_dl = ashareindex.AshareIndexDownload()
    ashareindex_dl.download_main()
    # 申万2021行业指数
    asharesw2021daily_dl = asharesw2021daily.AshareSW2021DailyDownload()
    asharesw2021daily_dl.download_main()
    # 期货数据
    futdaily_dl = futdaily.FutDailyDownload()
    futdaily_dl.download_main()
    # 财务数据
    asharefinance_dl = asharefinance.AshareFinanceDownload()
    asharefinance_dl.download_main()
    # # 如果积分只有2k，那就注释掉上面这行，运行以下函数(时间可能要一个多小时)
    # asharefinance_dl.download_main_code()
    return


def check_main():
    """
    检查今天的日志是否有错误，如果有发送到邮箱
    """
    today_date = datetime.datetime.today().strftime(r'%Y%m%d')
    log_dir = './log/' + today_date + '.log'
    flag_error = False
    with open(log_dir, 'r', encoding='utf-8') as log:
        for line in log:
            if 'ERROR' in line:
                flag_error = True
    if flag_error:
        logger = Logger('root')
        logger.send_error_email()


def run_daily_main():
    """
    每日运行主函数，包括以下步骤:
    1. 清除过去7天日志
    2. 下载数据
    3. 检查日志是否有错误，如果有则发送到邮箱
    """
    # step1: 清除过去7天日志
    clear_past_log_main()
    # step2: 下载数据
    download_main()
    # step3: 检查是否有error，有的话，发送到邮箱
    check_main()
