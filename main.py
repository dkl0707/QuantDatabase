'''
Author: dkl
Date: 2023-12-19 17:27:23
Description: 主函数
'''
import os
os.chdir(os.path.dirname(__file__))
# from main_func.initialize import initialize_main
# from main_func.pull_table_structure import pull_table_structure_main
from main_func.run_daily import run_daily_main
# from test.test_main import test_all_cases


if __name__ == '__main__':
    # # 是否拉取远程数据库的表结构
    # # 注意:除非在原有数据库中建好了表，要将表结构拉取到本地，否则都不调用
    # pull_table_structure_main()
    # # 是否创建数据库和数据表进行初始化
    # initialize_main()
    # 每日运行下载存储程序
    run_daily_main()
    # # 测试函数
    # test_all_cases(report_html=True)