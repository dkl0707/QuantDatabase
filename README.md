# QuantDatabase
金融量化数据库搭建，介绍见https://mp.weixin.qq.com/s/q9hravMG_v_EHm9APl-x7w

本项目为根据tushare数据库进行搭建，**所有配置信息如数据库信息，tushare token等均在config文件夹中的config.ini下。请换成自己的信息。**
项目代码仅作为初步示例使用，如还需加入更多的如期货、基金等，可以在download文件夹下编写新的如asharedaily.py的下载函数文件。
当然，如果你不是用tushare的接口，也可以在本代码框架下做相应的修改，例如，下载A股每日交易数据时，可将download中的asharedaily.py中downloader.download(pro.daily, trade_date=trade_date，fields=fields)的换成自己的数据接口。

安装步骤如下：
1. 确保服务器的python版本最好在3.8以上。如果是centos等自带的3.6，请进行升级。可以参考[https://blog.csdn.net/py_xiaoguaishou/article/details/127839998](https://blog.csdn.net/py_xiaoguaishou/article/details/127839998)
2. 安装mysql。可以用docker，也可以用yum。推荐安装mysql 8.0以上版本
3. 把本项目文件放到你的服务器上
4. 进入fin_download文件夹，执行"pip install -r requirements.txt"
5. 运行main.py文件，即"python main.py"，经过数小时下载即可在你的mysql上一键搭建好自己的数据库
6. 如果需要定时运行，可通过crontab操作。例如需要每天4点执行main.py，命令行输入"crontab -e"回车，在新开的crontab窗口下输入"0 4 * * * /usr/bin/python /home/aaa/QuantDatabase/main.py"，注意，这里main.py是绝对路径，而不是相对路径。

文件结构说明如下：
* config: 包含**config.ini(配置文件)**
* database: 包含**database.py(数据库的初始化)**
* download: **下载数据的核心函数**. 本项目中，仅包含A股每日数据下载(asharedaily.py)、交易日历下载(trade_cal.py)和从mysql中反向拉取数据表结构信息到本地(table_structure.py)。
* log: **日志文件存储**。当每天运行main.py时，该文件夹都会有相应的log文件。如"20230415.py"。
* table_structure: **数据库结构**。table_comment.xlsx是表的注释，table_index.xlsx是表的索引信息，table_structure.xlsx是表的字段信息。
* test: **测试函数**，使用unittest框架。包含cases文件夹，test_main.py和test_all_result.html。其中cases下是各个测试类，test_main.py是测试的主函数，里面包含了单个测试和全部测试的情况。main.py中，可以注释掉main()，取消注释test_main()进行测试。test_all_result.html是全部测试的结果，可以直接点击进行查看。
* utils: **工具性函数**。包含配置文件读取(conf.py), tushare数据下载器(downloader.py), 日志函数(logger.py), 邮件发送函数(sendemail.py)
* main.py：**主函数文件**

