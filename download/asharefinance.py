'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 财务数据下载
注意：我们的目标是要构建财务因子，假设使用最新四季数据，我们只需要更新最新四期
考虑到当前仍有公司还未发布财报，实际应该更新五期
备注: 这个函数运行需要5k积分，如果你积分2k。那么请按照股票列表的方式进行循环下载
即在run_daily.py中改成asharefinance_dl.download_main_code()
'''
import datetime
import pandas as pd
import tushare as ts
from database.database import DataBase
from utils.conf import Config
from utils.downloader import TushareDownloader
from utils.logger import logger_decorator, Logger
from typing import List
from tqdm import tqdm
from sqlalchemy.types import VARCHAR, DECIMAL, SMALLINT

# 获取token
tstoken = Config("tushare").get_config("tstoken")
pro = ts.pro_api(tstoken)
# 获取downloader
downloader = TushareDownloader()
# 获取日志记录器
logger = Logger("asharefinance")


class AshareFinanceDownload(DataBase):
    """
    每天的交易数据下载
    """

    def __init__(self, period_lst: List[str] = None, code_lst: List[str] = None):
        """
        类初始化.

        Parameters
        ----------
        period_lst: List[str]. 默认为None, 即交易日历中有，但表中没有的数据
        """
        super().__init__(database="stk_data")
        self.period_lst = period_lst
        self.code_lst = code_lst

    # #############################################################
    # 以下为按照报告期下载
    # #############################################################
    def _set_period_lst(self, table_name):
        """
        获取应该循环的交易日列表，其中self.trade_date_lst必须为None

        Parameters
        ----------
        table_name: 数据库表名
        """
        # 默认下载日期为至今
        if self.period_lst is not None:
            raise ValueError("self.trade_date_lst is not None!")
        # 数据库中最新日期
        sql = f"""select distinct a.end_date as period from {table_name} a;"""
        table_period_lst = pd.read_sql(sql=sql, con=self.engine)["period"].tolist()
        all_period_lst = self._get_all_period_lst()
        # 如果不考虑更新近五个报告期的操作，数据库中应该补充的日期
        period_lst1 = sorted(list(set(all_period_lst)-set(table_period_lst)))
        # 至昨天为止倒数五个报告期
        period_lst2 = all_period_lst[-5:]
        # 两个lst去重加总，即为需要更新的日期
        self.period_lst = sorted(list(set(period_lst1+period_lst2)))
        # # 备注：后来我们发现，tushare财务数据中公告日有一堆看不懂的bug(参考2017年的002069.SZ), 干脆直接放弃修正了。。。
        # # 想要的兄弟们还是参考wind或者ricequant/joinquant的数据，他们有提供PIT数据
        # # 报表类型映射:数据库-tushare
        # self.report_type_dct = {
        #     1: 1, # 修正后合并报表
        #     2: 5, # 修正前合并报表
        #     3: 3, # 修正后单季报表
        #     4: 2, # 修正前单季报表
        # }
        return

    def _get_all_period_lst(self):
        """
        获取至昨天为止历史所有报告期
        
        Returns
        -------
        List[str]
            至昨天为止历史所有报告期
        """
        now_datetime = datetime.datetime.now()
        yes_datetime = now_datetime - datetime.timedelta(days=1)
        yes_date = yes_datetime.strftime(r"%Y%m%d")

        # 到昨天为止,今年应该提取哪些报告期
        yes_year = yes_date[0:4]
        temp_lst = [yes_year + i for i in ["0331", "0630", "0930", "1231"]]
        yes_period_lst = []
        for period in temp_lst:
            if yes_date >= period:
                yes_period_lst.append(period)

        # 到去年年底为止，应该提取哪些报告期
        ly = int(yes_year) - 1
        ly_period_lst = []
        for year in range(1991, ly + 1):
            temp_lst = [str(year) + i for i in ["0331", "0630", "0930", "1231"]]
            ly_period_lst.extend(temp_lst)
        period_lst = sorted(ly_period_lst + yes_period_lst)
        return period_lst

    def _clear_hist5_data(self, table_name):
        """
        清除过去五期的数据
        """
        all_period_lst = self._get_all_period_lst()
        lst = [f"'{period}'" for period in all_period_lst[-5:]]
        string = ",".join(lst)
        sql = f"delete from {table_name} where end_date in ({string});"
        self.execute_sql(sql)
        return

    @logger_decorator(logger)
    def download_main(self):
        self.download_income()
        self.download_balancesheet()
        self.download_cashflow()

    @logger_decorator(logger)
    def download_income(self):
        """
        利润表数据下载
        """
        self._set_period_lst("ashareincome")
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "basic_eps",
            "diluted_eps",
            "total_revenue",
            "revenue",
            "int_income",
            "prem_earned",
            "comm_income",
            "n_commis_income",
            "n_oth_income",
            "n_oth_b_income",
            "prem_income",
            "out_prem",
            "une_prem_reser",
            "reins_income",
            "n_sec_tb_income",
            "n_sec_uw_income",
            "n_asset_mg_income",
            "oth_b_income",
            "fv_value_chg_gain",
            "invest_income",
            "ass_invest_income",
            "forex_gain",
            "total_cogs",
            "oper_cost",
            "int_exp",
            "comm_exp",
            "biz_tax_surchg",
            "sell_exp",
            "admin_exp",
            "fin_exp",
            "assets_impair_loss",
            "prem_refund",
            "compens_payout",
            "reser_insur_liab",
            "div_payt",
            "reins_exp",
            "oper_exp",
            "compens_payout_refu",
            "insur_reser_refu",
            "reins_cost_refund",
            "other_bus_cost",
            "operate_profit",
            "non_oper_income",
            "non_oper_exp",
            "nca_disploss",
            "total_profit",
            "income_tax",
            "n_income",
            "n_income_attr_p",
            "minority_gain",
            "oth_compr_income",
            "t_compr_income",
            "compr_inc_attr_p",
            "compr_inc_attr_m_s",
            "ebit",
            "ebitda",
            "insurance_exp",
            "undist_profit",
            "distable_profit",
            "rd_exp",
            "fin_exp_int_exp",
            "fin_exp_int_inc",
            "transfer_surplus_rese",
            "transfer_housing_imprest",
            "transfer_oth",
            "adj_lossgain",
            "withdra_legal_surplus",
            "withdra_legal_pubfund",
            "withdra_biz_devfund",
            "withdra_rese_fund",
            "withdra_oth_ersu",
            "workers_welfare",
            "distr_profit_shrhder",
            "prfshare_payable_dvd",
            "comshare_payable_dvd",
            "capit_comstock_div",
            "net_after_nr_lp_correct",
            "credit_impa_loss",
            "net_expo_hedging_benefits",
            "oth_impair_loss_assets",
            "total_opcost",
            "amodcost_fin_assets",
            "oth_income",
            "asset_disp_income",
            "continued_net_profit",
            "end_net_profit",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 过去五期的数据
        big5_df = pd.DataFrame()
        for period in tqdm(self.period_lst):
            df = pd.DataFrame()
            tempdf = downloader.download(
                pro.income_vip,
                period=period,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            tempdf = tempdf[list(sql_dtype.keys())].copy()
            df = pd.concat([df, tempdf])
            # 因为tushare的end_type数据不全，我们手动补上
            df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
            period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
            for md, period_type in period_type_dct.items():
                df.loc[df["end_period"] == md, "end_type"] = period_type
            df = df[list(sql_dtype.keys())].copy()
            df = df.sort_values(["end_date", "stock_code", "ann_date"])
            # 取公告日最新的数据
            df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
            df = df.reset_index(drop=True)
            # 现在有了这个数据，判断是否在前五期内
            # 如果不在，就直接存到数据库里
            if period < self.period_lst[-5]:
                self.store_data(
                    data=df,
                    data_name=f"股票利润表_{period}",
                    table_name="ashareincome",
                    dtype=sql_dtype
                )
            # 否则存到big5_df里面
            else:
                big5_df = pd.concat([big5_df, df], axis=0)

        # 启动事务，清除表内前五期数据，再将新的数据存入
        with self.engine.begin():
            self._clear_hist5_data("ashareincome")
            self.store_data(
                data=big5_df,
                data_name="股票利润表",
                table_name="ashareincome",
                dtype=sql_dtype
            )
        self.period_lst = None
        return

    @logger_decorator(logger)
    def download_balancesheet(self):
        """
        资产负债表数据下载
        """
        self._set_period_lst("asharebalancesheet")
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "total_share",
            "cap_rese",
            "undistr_porfit",
            "surplus_rese",
            "special_rese",
            "money_cap",
            "trad_asset",
            "notes_receiv",
            "accounts_receiv",
            "oth_receiv",
            "prepayment",
            "div_receiv",
            "int_receiv",
            "inventories",
            "amor_exp",
            "nca_within_1y",
            "sett_rsrv",
            "loanto_oth_bank_fi",
            "premium_receiv",
            "reinsur_receiv",
            "reinsur_res_receiv",
            "pur_resale_fa",
            "oth_cur_assets",
            "total_cur_assets",
            "fa_avail_for_sale",
            "htm_invest",
            "lt_eqt_invest",
            "invest_real_estate",
            "time_deposits",
            "oth_assets",
            "lt_rec",
            "fix_assets",
            "cip",
            "const_materials",
            "fixed_assets_disp",
            "produc_bio_assets",
            "oil_and_gas_assets",
            "intan_assets",
            "r_and_d",
            "goodwill",
            "lt_amor_exp",
            "defer_tax_assets",
            "decr_in_disbur",
            "oth_nca",
            "total_nca",
            "cash_reser_cb",
            "depos_in_oth_bfi",
            "prec_metals",
            "deriv_assets",
            "rr_reins_une_prem",
            "rr_reins_outstd_cla",
            "rr_reins_lins_liab",
            "rr_reins_lthins_liab",
            "refund_depos",
            "ph_pledge_loans",
            "refund_cap_depos",
            "indep_acct_assets",
            "client_depos",
            "client_prov",
            "transac_seat_fee",
            "invest_as_receiv",
            "total_assets",
            "lt_borr",
            "st_borr",
            "cb_borr",
            "depos_ib_deposits",
            "loan_oth_bank",
            "trading_fl",
            "notes_payable",
            "acct_payable",
            "adv_receipts",
            "sold_for_repur_fa",
            "comm_payable",
            "payroll_payable",
            "taxes_payable",
            "int_payable",
            "div_payable",
            "oth_payable",
            "acc_exp",
            "deferred_inc",
            "st_bonds_payable",
            "payable_to_reinsurer",
            "rsrv_insur_cont",
            "acting_trading_sec",
            "acting_uw_sec",
            "non_cur_liab_due_1y",
            "oth_cur_liab",
            "total_cur_liab",
            "bond_payable",
            "lt_payable",
            "specific_payables",
            "estimated_liab",
            "defer_tax_liab",
            "defer_inc_non_cur_liab",
            "oth_ncl",
            "total_ncl",
            "depos_oth_bfi",
            "deriv_liab",
            "depos",
            "agency_bus_liab",
            "oth_liab",
            "prem_receiv_adva",
            "depos_received",
            "ph_invest",
            "reser_une_prem",
            "reser_outstd_claims",
            "reser_lins_liab",
            "reser_lthins_liab",
            "indept_acc_liab",
            "pledge_borr",
            "indem_payable",
            "policy_div_payable",
            "total_liab",
            "treasury_share",
            "ordin_risk_reser",
            "forex_differ",
            "invest_loss_unconf",
            "minority_int",
            "total_hldr_eqy_exc_min_int",
            "total_hldr_eqy_inc_min_int",
            "total_liab_hldr_eqy",
            "lt_payroll_payable",
            "oth_comp_income",
            "oth_eqt_tools",
            "oth_eqt_tools_p_shr",
            "lending_funds",
            "acc_receivable",
            "st_fin_payable",
            "payables",
            "hfs_assets",
            "hfs_sales",
            "cost_fin_assets",
            "fair_value_fin_assets",
            "cip_total",
            "oth_pay_total",
            "long_pay_total",
            "debt_invest",
            "oth_debt_invest",
            "oth_eq_invest",
            "oth_illiq_fin_assets",
            "oth_eq_ppbond",
            "receiv_financing",
            "use_right_assets",
            "lease_liab",
            "contract_assets",
            "contract_liab",
            "accounts_receiv_bill",
            "accounts_pay",
            "oth_rcv_total",
            "fix_assets_total",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 过去五期的数据
        big5_df = pd.DataFrame()
        for period in tqdm(self.period_lst):
            df = pd.DataFrame()
            tempdf = downloader.download(
                pro.balancesheet_vip,
                period=period,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            df = pd.concat([df, tempdf])
            # 因为tushare的end_type数据不全，我们手动补上
            df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
            period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
            for md, period_type in period_type_dct.items():
                df.loc[df["end_period"] == md, "end_type"] = period_type
            df = df[list(sql_dtype.keys())].copy()
            df = df.sort_values(["end_date", "stock_code", "ann_date"])
            # 取公告日最新的数据
            df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
            df = df.reset_index(drop=True)
            # 现在有了这个数据，判断是否在前五期内
            # 如果不在，就直接存到数据库里
            if period < self.period_lst[-5]:
                self.store_data(
                    data=df,
                    data_name=f"股票资产负债表_{period}",
                    table_name="asharebalancesheet",
                    dtype=sql_dtype
                )
            # 否则存到big5_df里面
            else:
                big5_df = pd.concat([big5_df, df], axis=0)

        # 启动事务，清除表内前五期数据，再将新的数据存入
        with self.engine.begin():
            self._clear_hist5_data("asharebalancesheet")
            self.store_data(
                data=big5_df,
                data_name="股票资产负债表",
                table_name="asharebalancesheet",
                dtype=sql_dtype
            )
        self.period_lst = None
        return

    @logger_decorator(logger)
    def download_cashflow(self):
        self._set_period_lst("asharecashflow")
        # 现金流量表数据下载
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "net_profit",
            "finan_exp",
            "c_fr_sale_sg",
            "recp_tax_rends",
            "n_depos_incr_fi",
            "n_incr_loans_cb",
            "n_inc_borr_oth_fi",
            "prem_fr_orig_contr",
            "n_incr_insured_dep",
            "n_reinsur_prem",
            "n_incr_disp_tfa",
            "ifc_cash_incr",
            "n_incr_disp_faas",
            "n_incr_loans_oth_bank",
            "n_cap_incr_repur",
            "c_fr_oth_operate_a",
            "c_inf_fr_operate_a",
            "c_paid_goods_s",
            "c_paid_to_for_empl",
            "c_paid_for_taxes",
            "n_incr_clt_loan_adv",
            "n_incr_dep_cbob",
            "c_pay_claims_orig_inco",
            "pay_handling_chrg",
            "pay_comm_insur_plcy",
            "oth_cash_pay_oper_act",
            "st_cash_out_act",
            "n_cashflow_act",
            "oth_recp_ral_inv_act",
            "c_disp_withdrwl_invest",
            "c_recp_return_invest",
            "n_recp_disp_fiolta",
            "n_recp_disp_sobu",
            "stot_inflows_inv_act",
            "c_pay_acq_const_fiolta",
            "c_paid_invest",
            "n_disp_subs_oth_biz",
            "oth_pay_ral_inv_act",
            "n_incr_pledge_loan",
            "stot_out_inv_act",
            "n_cashflow_inv_act",
            "c_recp_borrow",
            "proc_issue_bonds",
            "oth_cash_recp_ral_fnc_act",
            "stot_cash_in_fnc_act",
            "free_cashflow",
            "c_prepay_amt_borr",
            "c_pay_dist_dpcp_int_exp",
            "incl_dvd_profit_paid_sc_ms",
            "oth_cashpay_ral_fnc_act",
            "stot_cashout_fnc_act",
            "n_cash_flows_fnc_act",
            "eff_fx_flu_cash",
            "n_incr_cash_cash_equ",
            "c_cash_equ_beg_period",
            "c_cash_equ_end_period",
            "c_recp_cap_contrib",
            "incl_cash_rec_saims",
            "uncon_invest_loss",
            "prov_depr_assets",
            "depr_fa_coga_dpba",
            "amort_intang_assets",
            "lt_amort_deferred_exp",
            "decr_deferred_exp",
            "incr_acc_exp",
            "loss_disp_fiolta",
            "loss_scr_fa",
            "loss_fv_chg",
            "invest_loss",
            "decr_def_inc_tax_assets",
            "incr_def_inc_tax_liab",
            "decr_inventories",
            "decr_oper_payable",
            "incr_oper_payable",
            "others",
            "im_net_cashflow_oper_act",
            "conv_debt_into_cap",
            "conv_copbonds_due_within_1y",
            "fa_fnc_leases",
            "im_n_incr_cash_equ",
            "net_dism_capital_add",
            "net_cash_rece_sec",
            "credit_impa_loss",
            "use_right_asset_dep",
            "oth_loss_asset",
            "end_bal_cash",
            "beg_bal_cash",
            "end_bal_cash_equ",
            "beg_bal_cash_equ",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 过去五期的数据
        big5_df = pd.DataFrame()
        for period in tqdm(self.period_lst):
            df = pd.DataFrame()
            tempdf = downloader.download(
                pro.cashflow_vip,
                period=period,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            df = pd.concat([df, tempdf])
            # 因为tushare的end_type数据不全，我们手动补上
            df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
            period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
            for md, period_type in period_type_dct.items():
                df.loc[df["end_period"] == md, "end_type"] = period_type
            df = df[list(sql_dtype.keys())].copy()
            df = df.sort_values(["end_date", "stock_code", "ann_date"])
            # 取公告日最新的数据
            df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
            df = df.reset_index(drop=True)
            # 现在有了这个数据，判断是否在前五期内
            # 如果不在，就直接存到数据库里
            if period < self.period_lst[-5]:
                self.store_data(
                    data=df,
                    data_name=f"股票现金流量表_{period}",
                    table_name="asharecashflow",
                    dtype=sql_dtype
                )
            # 否则存到big5_df里面
            else:
                big5_df = pd.concat([big5_df, df], axis=0)

        # 启动事务，清除表内前五期数据，再将新的数据存入
        with self.engine.begin():
            self._clear_hist5_data("asharecashflow")
            self.store_data(
                data=big5_df,
                data_name="股票现金流量表",
                table_name="asharecashflow",
                dtype=sql_dtype
            )
        self.period_lst = None
        return

    # #############################################################
    # 以下部分为按照stock_code方式下载
    # #############################################################
    def _set_code_lst(self):
        sql = "select stock_code from asharestockbasic;"
        code_lst = pd.read_sql(sql=sql, con=self.engine)["stock_code"].tolist()
        self.code_lst = code_lst

    @logger_decorator(logger)
    def download_main_code(self):
        self.download_income_code()
        self.download_balancesheet_code()
        self.download_cashflow_code()

    @logger_decorator(logger)
    def download_income_code(self):
        """
        利润表数据下载
        """
        self._set_code_lst()
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "basic_eps",
            "diluted_eps",
            "total_revenue",
            "revenue",
            "int_income",
            "prem_earned",
            "comm_income",
            "n_commis_income",
            "n_oth_income",
            "n_oth_b_income",
            "prem_income",
            "out_prem",
            "une_prem_reser",
            "reins_income",
            "n_sec_tb_income",
            "n_sec_uw_income",
            "n_asset_mg_income",
            "oth_b_income",
            "fv_value_chg_gain",
            "invest_income",
            "ass_invest_income",
            "forex_gain",
            "total_cogs",
            "oper_cost",
            "int_exp",
            "comm_exp",
            "biz_tax_surchg",
            "sell_exp",
            "admin_exp",
            "fin_exp",
            "assets_impair_loss",
            "prem_refund",
            "compens_payout",
            "reser_insur_liab",
            "div_payt",
            "reins_exp",
            "oper_exp",
            "compens_payout_refu",
            "insur_reser_refu",
            "reins_cost_refund",
            "other_bus_cost",
            "operate_profit",
            "non_oper_income",
            "non_oper_exp",
            "nca_disploss",
            "total_profit",
            "income_tax",
            "n_income",
            "n_income_attr_p",
            "minority_gain",
            "oth_compr_income",
            "t_compr_income",
            "compr_inc_attr_p",
            "compr_inc_attr_m_s",
            "ebit",
            "ebitda",
            "insurance_exp",
            "undist_profit",
            "distable_profit",
            "rd_exp",
            "fin_exp_int_exp",
            "fin_exp_int_inc",
            "transfer_surplus_rese",
            "transfer_housing_imprest",
            "transfer_oth",
            "adj_lossgain",
            "withdra_legal_surplus",
            "withdra_legal_pubfund",
            "withdra_biz_devfund",
            "withdra_rese_fund",
            "withdra_oth_ersu",
            "workers_welfare",
            "distr_profit_shrhder",
            "prfshare_payable_dvd",
            "comshare_payable_dvd",
            "capit_comstock_div",
            "net_after_nr_lp_correct",
            "credit_impa_loss",
            "net_expo_hedging_benefits",
            "oth_impair_loss_assets",
            "total_opcost",
            "amodcost_fin_assets",
            "oth_income",
            "asset_disp_income",
            "continued_net_profit",
            "end_net_profit",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 总的数据
        df = pd.DataFrame()
        for stock_code in tqdm(self.code_lst):   
            tempdf = downloader.download(
                pro.income,
                ts_code=stock_code,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            tempdf = tempdf[list(sql_dtype.keys())].copy()
            df = pd.concat([df, tempdf])
        # 因为tushare的end_type数据不全，我们手动补上
        df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
        period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
        for md, period_type in period_type_dct.items():
            df.loc[df["end_period"] == md, "end_type"] = period_type
        df = df[list(sql_dtype.keys())].copy()
        df = df.sort_values(["end_date", "stock_code", "ann_date"])
        # 取公告日最新的数据
        df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
        df = df.reset_index(drop=True)
        self.store_data(
            data=df,
            data_name="股票利润表",
            table_name="ashareincome",
            dtype=sql_dtype,
            flag_replace=True
        )
        self.code_lst = None
        return

    @logger_decorator(logger)
    def download_balancesheet_code(self):
        """
        资产负债表数据下载
        """
        self._set_code_lst()
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "total_share",
            "cap_rese",
            "undistr_porfit",
            "surplus_rese",
            "special_rese",
            "money_cap",
            "trad_asset",
            "notes_receiv",
            "accounts_receiv",
            "oth_receiv",
            "prepayment",
            "div_receiv",
            "int_receiv",
            "inventories",
            "amor_exp",
            "nca_within_1y",
            "sett_rsrv",
            "loanto_oth_bank_fi",
            "premium_receiv",
            "reinsur_receiv",
            "reinsur_res_receiv",
            "pur_resale_fa",
            "oth_cur_assets",
            "total_cur_assets",
            "fa_avail_for_sale",
            "htm_invest",
            "lt_eqt_invest",
            "invest_real_estate",
            "time_deposits",
            "oth_assets",
            "lt_rec",
            "fix_assets",
            "cip",
            "const_materials",
            "fixed_assets_disp",
            "produc_bio_assets",
            "oil_and_gas_assets",
            "intan_assets",
            "r_and_d",
            "goodwill",
            "lt_amor_exp",
            "defer_tax_assets",
            "decr_in_disbur",
            "oth_nca",
            "total_nca",
            "cash_reser_cb",
            "depos_in_oth_bfi",
            "prec_metals",
            "deriv_assets",
            "rr_reins_une_prem",
            "rr_reins_outstd_cla",
            "rr_reins_lins_liab",
            "rr_reins_lthins_liab",
            "refund_depos",
            "ph_pledge_loans",
            "refund_cap_depos",
            "indep_acct_assets",
            "client_depos",
            "client_prov",
            "transac_seat_fee",
            "invest_as_receiv",
            "total_assets",
            "lt_borr",
            "st_borr",
            "cb_borr",
            "depos_ib_deposits",
            "loan_oth_bank",
            "trading_fl",
            "notes_payable",
            "acct_payable",
            "adv_receipts",
            "sold_for_repur_fa",
            "comm_payable",
            "payroll_payable",
            "taxes_payable",
            "int_payable",
            "div_payable",
            "oth_payable",
            "acc_exp",
            "deferred_inc",
            "st_bonds_payable",
            "payable_to_reinsurer",
            "rsrv_insur_cont",
            "acting_trading_sec",
            "acting_uw_sec",
            "non_cur_liab_due_1y",
            "oth_cur_liab",
            "total_cur_liab",
            "bond_payable",
            "lt_payable",
            "specific_payables",
            "estimated_liab",
            "defer_tax_liab",
            "defer_inc_non_cur_liab",
            "oth_ncl",
            "total_ncl",
            "depos_oth_bfi",
            "deriv_liab",
            "depos",
            "agency_bus_liab",
            "oth_liab",
            "prem_receiv_adva",
            "depos_received",
            "ph_invest",
            "reser_une_prem",
            "reser_outstd_claims",
            "reser_lins_liab",
            "reser_lthins_liab",
            "indept_acc_liab",
            "pledge_borr",
            "indem_payable",
            "policy_div_payable",
            "total_liab",
            "treasury_share",
            "ordin_risk_reser",
            "forex_differ",
            "invest_loss_unconf",
            "minority_int",
            "total_hldr_eqy_exc_min_int",
            "total_hldr_eqy_inc_min_int",
            "total_liab_hldr_eqy",
            "lt_payroll_payable",
            "oth_comp_income",
            "oth_eqt_tools",
            "oth_eqt_tools_p_shr",
            "lending_funds",
            "acc_receivable",
            "st_fin_payable",
            "payables",
            "hfs_assets",
            "hfs_sales",
            "cost_fin_assets",
            "fair_value_fin_assets",
            "cip_total",
            "oth_pay_total",
            "long_pay_total",
            "debt_invest",
            "oth_debt_invest",
            "oth_eq_invest",
            "oth_illiq_fin_assets",
            "oth_eq_ppbond",
            "receiv_financing",
            "use_right_assets",
            "lease_liab",
            "contract_assets",
            "contract_liab",
            "accounts_receiv_bill",
            "accounts_pay",
            "oth_rcv_total",
            "fix_assets_total",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 总的数据
        df = pd.DataFrame()
        for stock_code in tqdm(self.code_lst):   
            tempdf = downloader.download(
                pro.balancesheet,
                ts_code=stock_code,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            df = pd.concat([df, tempdf])
        # 因为tushare的end_type数据不全，我们手动补上
        df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
        period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
        for md, period_type in period_type_dct.items():
            df.loc[df["end_period"] == md, "end_type"] = period_type
        df = df[list(sql_dtype.keys())].copy()
        df = df.sort_values(["end_date", "stock_code", "ann_date"])
        # 取公告日最新的数据
        df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
        df = df.reset_index(drop=True)
        self.store_data(
            data=df,
            data_name="股票资产负债表",
            table_name="asharebalancesheet",
            dtype=sql_dtype,
            flag_replace=True
        )
        self.code_lst = None
        return

    @logger_decorator(logger)
    def download_cashflow_code(self):
        self._set_code_lst()
        # 现金流量表数据下载
        # 变量列表
        var_name_lst = [
            "stock_code",
            "ann_date",
            "end_date",
            "comp_type",
            "end_type",
            "net_profit",
            "finan_exp",
            "c_fr_sale_sg",
            "recp_tax_rends",
            "n_depos_incr_fi",
            "n_incr_loans_cb",
            "n_inc_borr_oth_fi",
            "prem_fr_orig_contr",
            "n_incr_insured_dep",
            "n_reinsur_prem",
            "n_incr_disp_tfa",
            "ifc_cash_incr",
            "n_incr_disp_faas",
            "n_incr_loans_oth_bank",
            "n_cap_incr_repur",
            "c_fr_oth_operate_a",
            "c_inf_fr_operate_a",
            "c_paid_goods_s",
            "c_paid_to_for_empl",
            "c_paid_for_taxes",
            "n_incr_clt_loan_adv",
            "n_incr_dep_cbob",
            "c_pay_claims_orig_inco",
            "pay_handling_chrg",
            "pay_comm_insur_plcy",
            "oth_cash_pay_oper_act",
            "st_cash_out_act",
            "n_cashflow_act",
            "oth_recp_ral_inv_act",
            "c_disp_withdrwl_invest",
            "c_recp_return_invest",
            "n_recp_disp_fiolta",
            "n_recp_disp_sobu",
            "stot_inflows_inv_act",
            "c_pay_acq_const_fiolta",
            "c_paid_invest",
            "n_disp_subs_oth_biz",
            "oth_pay_ral_inv_act",
            "n_incr_pledge_loan",
            "stot_out_inv_act",
            "n_cashflow_inv_act",
            "c_recp_borrow",
            "proc_issue_bonds",
            "oth_cash_recp_ral_fnc_act",
            "stot_cash_in_fnc_act",
            "free_cashflow",
            "c_prepay_amt_borr",
            "c_pay_dist_dpcp_int_exp",
            "incl_dvd_profit_paid_sc_ms",
            "oth_cashpay_ral_fnc_act",
            "stot_cashout_fnc_act",
            "n_cash_flows_fnc_act",
            "eff_fx_flu_cash",
            "n_incr_cash_cash_equ",
            "c_cash_equ_beg_period",
            "c_cash_equ_end_period",
            "c_recp_cap_contrib",
            "incl_cash_rec_saims",
            "uncon_invest_loss",
            "prov_depr_assets",
            "depr_fa_coga_dpba",
            "amort_intang_assets",
            "lt_amort_deferred_exp",
            "decr_deferred_exp",
            "incr_acc_exp",
            "loss_disp_fiolta",
            "loss_scr_fa",
            "loss_fv_chg",
            "invest_loss",
            "decr_def_inc_tax_assets",
            "incr_def_inc_tax_liab",
            "decr_inventories",
            "decr_oper_payable",
            "incr_oper_payable",
            "others",
            "im_net_cashflow_oper_act",
            "conv_debt_into_cap",
            "conv_copbonds_due_within_1y",
            "fa_fnc_leases",
            "im_n_incr_cash_equ",
            "net_dism_capital_add",
            "net_cash_rece_sec",
            "credit_impa_loss",
            "use_right_asset_dep",
            "oth_loss_asset",
            "end_bal_cash",
            "beg_bal_cash",
            "end_bal_cash_equ",
            "beg_bal_cash_equ",
        ]
        fields_lst = var_name_lst.copy()
        fields_lst[fields_lst.index("stock_code")] = "ts_code"
        fields = ",".join(fields_lst)
        # 设置变量类型
        sql_dtype_lst = (
            [VARCHAR(255)] * 3
            + [SMALLINT] * 3
            + [DECIMAL(20, 4)] * (len(fields_lst) - 6)
        )
        sql_dtype = dict(zip(var_name_lst, sql_dtype_lst))
        # 总的数据
        df = pd.DataFrame()
        for stock_code in tqdm(self.code_lst):   
            tempdf = downloader.download(
                pro.cashflow,
                ts_code=stock_code,
                report_type=1,
                fields=fields,
            )
            tempdf = tempdf.rename(columns={"ts_code": "stock_code"})
            df = pd.concat([df, tempdf])
        # 因为tushare的end_type数据不全，我们手动补上
        df["end_period"] = df["end_date"].apply(lambda x: x[-4:])
        period_type_dct = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}
        for md, period_type in period_type_dct.items():
            df.loc[df["end_period"] == md, "end_type"] = period_type
        df = df[list(sql_dtype.keys())].copy()
        df = df.sort_values(["end_date", "stock_code", "ann_date"])
        # 取公告日最新的数据
        df = df.drop_duplicates(["end_date", "stock_code"], keep='last')
        df = df.reset_index(drop=True)
        self.store_data(
            data=df,
            data_name="股票现金流量表",
            table_name="asharecashflow",
            dtype=sql_dtype,
            flag_replace=True
        )
        self.code_lst = None
        return