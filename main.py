'''
Author: dkl
Description: 主文件爬取
Date: 2022-11-22 10:55:32
'''
import os
import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
today_date = datetime.datetime.today().strftime(r'%Y%m%d')


# 测试模块
def test_main(flag_simple=True):
    if flag_simple:
        from test.test_main import test_simple_case
        test_simple_case()
    else:
        from test.test_main import test_all_cases
        test_all_cases(report_html=True)
    return


def get_remote_table_structure():
    from download import table_structure
    table_structure.TableStructure().download_all_structure(
        rename_old_table=True, remove_past_days=7)
    return


# 主函数模块
def main():
    from database.database import DataBase
    from download import tradecal, asharedaily, asharesw2021daily, asharemonthly
    from utils.logger import Logger
    from utils.conf import Config
    from download.futdaily import FutDailyDownload

    # step0: 清除过去7天日志
    try:
        conf = Config('log')
        remove_past_days = int(conf.get_config('clean_past_log_days'))
        _clean_past_log(remove_past_days=remove_past_days)
    except Exception as e:
        raise ValueError('清除日志失败，请检查文件夹路径是否正确!\n' + e)
    # step1: 创建数据库
    db_table_dct = {
        'stk_data': [
            'asharedailybasic', 'asharedailyprices', 'asharestockbasic',
            'asharetradecal', 'asharesw2021daily', 'asharemonthlyprices',
        ],
        'fut_data': [
            'futbasic', 'futdailyprices', 'futholdingcffex', 'futholdingczce',
            'futholdingdce', 'futholdingshfe', 'futwsr'
        ]
    }
    for db_name, table_lst in db_table_dct.items():
        db_mysql = DataBase('mysql')
        db_mysql.create_database(db_name)
        for table_name in table_lst:
            db = DataBase(db_name)
            db.create_table(table_name)
    asharemonthly_dl = asharemonthly.AshareMonthlyDownload()
    asharemonthly_dl.download_monthlyprices()
    # step3: 下载数据
    FutDailyDownload().download_main()
    # 交易日历
    asharetradecal_dl = tradecal.TradecalDownload()
    asharetradecal_dl.download_main()
    # A股日频
    asharedaily_dl = asharedaily.AshareDailyDownload()
    asharedaily_dl.download_main()
    # 申万2021行业指数
    asharesw2021daily_dl = asharesw2021daily.AshareSW2021DailyDownload()
    asharesw2021daily_dl.download_main()

    # step4: 检查是否有error，有的话，发送到邮箱
    flag_error = _check_log_error()
    if flag_error:
        logger = Logger('root')
        logger.send_error_email()


def _clean_past_log(remove_past_days):
    '''
    Description
    ----------
    去除过去past_days的日志

    Parameters
    ----------
    remove_past_days: int. 指定天数

    Return
    ----------
    None.
    '''
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


def _check_log_error(log_dir=None):
    '''
    Description
    ----------
    检查日志是否有错误

    Parameters
    ----------
    log_dir: str. 默认为None. 即为当天日期

    Return
    ----------
    Bool. 日志是否有错误，有返回True，无返回False
    '''
    if log_dir is None:
        log_dir = './log/' + today_date + '.log'
    with open(log_dir, 'r', encoding='utf-8') as log:
        for line in log:
            if 'ERROR' in line:
                return True
    return False


if __name__ == '__main__':
    # get_remote_table_structure()
    main()
    # test_main(flag_simple=False)
    # db = AshareMonthlyDownload()
