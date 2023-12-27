'''
Author: dkl
Date: 2023-12-19 16:46:38
Description: 拉取远程数据库中所有已经创建的表到本地
'''
from database import table_structure
from utils.conf import Config


def pull_table_structure_main():
    """
    拉取远程数据库中所有已经创建的表到本地，并暂时保存原有的表
    失效期由config.ini的clear_past_table_structure_days决定
    """
    conf = Config('table_structure')
    clear_past_days = int(conf.get_config('clear_past_table_structure_days'))
    ts = table_structure.TableStructure()
    ts.pull_all_structure(rename_old_table=True,
                          clear_past_days=clear_past_days)
    return