# -*- coding: utf-8 -*-
"""
@Time    : 2019/7/29 14:37
@Software: PyCharm
@Author  : ykp
V1.0     : 实现评级结果合并.主要实现2个功能.
"""
import os
import copy
import pickle
import time
import datetime
import numpy as np
import pandas as pd
# from numba import jit  # 加速迭代,提高运行效率


def save_with_pickle_ykp(save_path, save_name, save_obj):
    # save variables in pickle-file after running codes
    """
    Input variables:
        save_path  ——  pickle文件的保存路径
        save_name  ——  pickle文件的名称
        save_obj   ——  pickle文件中所要保存的变量名,eg: [ {'data_features':data_features,'data_label':data_label} ]
    To use this function, coding:
        save_with_pickle_ykp( save_path, save_name, save_obj )
    """
    with open(save_path + save_name + '.p', "wb") as file_out:
        pickle.dump(save_obj, file_out)
    """
    Output variables:
        保存的pickle文件详见: 对应于save_path路径下的pickle文件
    """


def load_pickle_ykp(save_path, save_name):
    # load results in pickle-file
    """
    Input variables:
        save_path  ——  pickle文件的保存路径
        save_name  ——  pickle文件的名称
    To use this function, coding:
        load_pickle_ykp(save_path, save_name)
    """
    with open(save_path + save_name + '.p', "rb") as file_in:
        All_results = pickle.load(file_in)
    """
    Output variables:
        All_results —— 将save_path路径下的pickle文件内容读取出来
    """
    return All_results


def main():
    time_start = time.time()  # 记录初始时间
    path_in = os.getcwd() + '\\'  # 自动获取输入文件的路径
    # path_in = "H:\\self_learning\\python\\33.债券评级处理\\"  # 自定义输入文件的路径

    # ===01.读取评级含义数据===
    file_name_of_rating_meaning = "2019.07.26.信用评级定义一览.xls"  # 评级定义数据的excel文件名
    data_rating_meaning = pd.read_excel(path_in + "Data_in\\" + file_name_of_rating_meaning, sheet_name="万得")  # 读取评级定义的数据
    # columns_rating_meaning = list(data_rating_meaning.columns)  # 评级定义数据的所有列名

    # ===02.读取债项评级信息(也就是每笔债券对应的评级信息)===
    file_name_in_bond = "2019.07.26.所有债项评级(去重).csv"  # 债项评级数据的excel文件名
    data_bond = pd.read_csv(path_in + "Data_in\\" + file_name_in_bond, engine='python', encoding="utf-8-sig")  # 读取债项评级的数据
    # columns_bond = list(data_bond.columns)  # 债项评级数据的所有列名
    data_bond.columns = ['del_1', '证券代码', '证券简称', '信用评级', '评级类型', '评估机构', '债项评级时间']  # 将债项评级的列名与评级定义列名统一，方便合并
    # 以下注释的3行,是将评估机构的名称进行统一,防止合并时因评估机构名称略有不同而出错.这一步也可直接在原始excel改.
    data_bond.loc[data_bond['评估机构'] == '上海资信有限公司', '评估机构'] = "上海新世纪资信评估投资服务有限公司"
    data_bond.loc[data_bond['评估机构'] == '标普全球信用评级管理服务(上海)有限公司', '评估机构'] = "标准普尔评级服务公司"
    data_bond.loc[data_bond['评估机构'] == '中国诚信信用管理股份有限公司', '评估机构'] = "中诚信证券评估有限公司"

    # ===03.读取债券发行主体评级信息(也就是债券发行主体企业的评级信息)===
    file_name_in_firm = "2019.07.26.所有发债主体评级(去重).csv"
    data_firm = pd.read_csv(path_in + "Data_in\\" + file_name_in_firm, engine='python', encoding="utf-8-sig")
    # columns_firm = list(data_firm.columns)  # 债券发行主体评级数据的所有列名
    data_firm.columns = ['del_2', '证券代码', '证券简称', '信用评级', '评级类型', '评估机构', "发债主体评级预期", '发债主体评级时间']  # 将债券发行主体评级的列名与评级定义列名统一，方便合并
    # 以下注释的3行,是将评估机构的名称进行统一,防止合并时因评估机构名称略有不同而出错.这一步也可直接在原始excel改.
    data_firm.loc[data_firm['评估机构'] == '上海资信有限公司', '评估机构'] = "上海新世纪资信评估投资服务有限公司"
    data_firm.loc[data_firm['评估机构'] == '标普全球信用评级管理服务(上海)有限公司', '评估机构'] = "标准普尔评级服务公司"
    data_firm.loc[data_firm['评估机构'] == '中国诚信信用管理股份有限公司', '评估机构'] = "中诚信证券评估有限公司"

    # ===04.将评级定义['投资等级', '等级含义']对应填补到债项评级和债券发行主体评级的后2列===
    # ---04.1.将评级定义['投资等级', '等级含义']对应填补到债项评级,并输出csv文件
    columns_merge = ['信用评级', '评级类型', '评估机构']  # 对应填补时需要参考的基准列
    data_bond_rating = data_rating_meaning.merge(data_bond, how='right', on=columns_merge)  # 对应填补到债项评级
    names_bond_rating = ['证券代码', '证券简称', '信用评级', '评级类型', '评估机构', '债项评级时间', '投资等级', '等级含义']  # 将列名规范定义,前后保持一致
    data_bond_rating.to_csv(path_in + "Data_out\\" + '所有债项评级及等级含义输出(同时同债券不同评级机构结果在不同行).csv', columns=names_bond_rating, encoding='utf-8-sig')  # 将填补定义后的债项评级数据输出
    # ---04.2.将评级定义['投资等级', '等级含义']对应填补到债项评级,并输出csv文件
    data_firm_rating = data_rating_meaning.merge(data_firm, how='right', on=columns_merge)  # 对应填补到债券发行主体
    names_firm_rating = ['证券代码', '证券简称', '信用评级', '评级类型', '评估机构', '发债主体评级时间', '发债主体评级预期', '投资等级', '等级含义']  # 将列名规范定义,前后保持一致
    data_firm_rating.to_csv(path_in + "Data_out\\" + '所有债券发行主体评级及等级含义输出(同时同债券不同评级机构结果在不同行).csv', columns=names_firm_rating, encoding='utf-8-sig')  # 将填补定义后的债券发行主体评级数据输出
    # 以下注释这一行是为了看是否还存在评级含义为空的行,方便查漏补缺
    data_firm_rating[data_firm_rating["等级含义"].isin([np.nan])].to_csv(path_in + "Data_out\\" + '债券发行主体评级及等级含义（空）.csv', columns=names_firm_rating, encoding='utf-8-sig')

    # ===05.生成唯一标识码,方便之后进行匹配===
    # ===05.1.将债项评级信息生成唯一标识码===
    data_bond_rating['季度1'] = data_bond_rating["债项评级时间"].map(lambda x: str(int(np.floor(1+(time.strptime(str(x), "%Y%m%d").tm_mon-1)/3))))
    data_bond_rating['季度'] = data_bond_rating["债项评级时间"].map(lambda x: "第"+str(int(np.floor(1 + (time.strptime(str(x), "%Y%m%d").tm_mon - 1) / 3)))+"季度")
    data_bond_rating['年份'] = data_bond_rating["债项评级时间"].map(lambda x: time.strptime(str(x), "%Y%m%d").tm_year)
    data_bond_rating['债券唯一标识(年份-季度-证券代码)'] = data_bond_rating["年份"].map(str) +"-"+ data_bond_rating['季度1'].map(str)+"-"+data_bond_rating['证券代码'].map(str)  # 生成唯一标识列
    # ===05.2.将债券发行主体评级信息生成唯一标识码===
    data_firm_rating['季度1'] = data_firm_rating["发债主体评级时间"].map(lambda x: str(int(np.floor(1 + (time.strptime(str(x), "%Y%m%d").tm_mon - 1) / 3))))
    data_firm_rating['季度'] = data_firm_rating["发债主体评级时间"].map(lambda x: "第" + str(int(np.floor(1 + (time.strptime(str(x), "%Y%m%d").tm_mon - 1) / 3))) + "季度")
    data_firm_rating['年份'] = data_firm_rating["发债主体评级时间"].map(lambda x: time.strptime(str(x), "%Y%m%d").tm_year)
    data_firm_rating['债券唯一标识(年份-季度-证券代码)'] = data_firm_rating["年份"].map(str) + "-" + data_firm_rating['季度1'].map(str) + "-" + data_firm_rating['证券代码'].map(str)  # 生成唯一标识列

    # 以上虽然分别匹配上了债项和债券发行主体的评级信息,但由于一笔债券同一时间可能对应多家评级机构的评级,因此需要将同一时间的评级都转化为1行内容
    # ===06.债项评级整理输出===
    data_bond_rating.columns = ['债券评级评估机构', '债券评级评级类型', '债券评级投资等级',
                                '债券评级信用等级', '债券评级等级含义', '债券评级del_1',
                                '证券代码', '证券简称', '债券评级发布时间', '债券评级-季度1',
                                '债券评级-季度', '年份', '债券唯一标识(年份-季度-证券代码)']
    data_bond_rating_col = ['债券唯一标识(年份-季度-证券代码)', "债券评级发布时间",
                            "债券评级评级类型", "债券评级信用等级", "债券评级评估机构",
                            "债券评级投资等级", "债券评级等级含义"]
    data_bond_rating_2 = data_bond_rating[data_bond_rating_col]  # 只提取有用信息列
    single_code = list(set(data_bond_rating_2["债券唯一标识(年份-季度-证券代码)"]))  # 将所有唯一标识记录下来
    single_company = list(set(data_bond_rating_2["债券评级评估机构"]))  # 将所有评估机构名称记录下来
    columns_rate_02_new = ["{}({})".format(y, x) for x in single_company for y in data_bond_rating_col[1:]]  # 将信息列指标按评估机构进行区分
    data_rate_02_new = pd.DataFrame(columns=["债券唯一标识(年份-季度-证券代码)"] + columns_rate_02_new)  # 构建新的债项评级数据空表
    # ===06.1.开始按照债券代码进行遍历循环,所有债项评级及等级含义输出(唯一标识码+分评估机构)===
    for indices_i, i in enumerate(single_code):
        time_mid = time.time()
        print("\r 债项评级整理-程序运行进度:{:.2%}<{:.4f}>小时, 预计还有{:.4f}小时...".format(
            (indices_i + 1) / len(single_code),
            (time_mid - time_start) / 3600,
            (time_mid - time_start) / 3600 / (indices_i + 1) * (len(single_code) - (indices_i + 1))),
            end="")
        temp_i_df = pd.DataFrame()
        temp_i_df["债券唯一标识(年份-季度-证券代码)"] = [i]
        for j in single_company:
            temp_df = data_bond_rating_2.loc[(data_bond_rating_2["债券唯一标识(年份-季度-证券代码)"].isin([i])) & (data_bond_rating_2["债券评级评估机构"].isin([j])), :]
            if int(temp_df.shape[0]) >= 2:
                temp_df.drop_duplicates(subset=["债券唯一标识(年份-季度-证券代码)"], keep='first', inplace=True)
            temp_df.rename(columns=lambda x: x.replace(x, "{}({})".format(x, str(j))), inplace=True)
            temp_df.rename(columns={"债券唯一标识(年份-季度-证券代码)({})".format(str(j)): "债券唯一标识(年份-季度-证券代码)"}, inplace=True)
            temp_i_df = pd.merge(temp_i_df, temp_df, how="outer", on="债券唯一标识(年份-季度-证券代码)")
        data_rate_02_new = pd.concat([data_rate_02_new, temp_i_df], ignore_index=True)
    data_rate_02_new.to_csv(path_in + "Data_out\\" + '所有债项评级及等级含义输出(唯一标识码+分评估机构).csv', columns=["债券唯一标识(年份-季度-证券代码)"] + columns_rate_02_new, encoding='utf-8-sig')

    # ===07.债券发行主体评级整理输出===
    data_firm_rating.columns = ['发债主体评级评估机构', '发债主体评级类型', '发债主体评级投资等级',
                                '发债主体评级信用等级', '发债主体评级等级含义', '发债主体评级del_2',
                                '证券代码', '证券简称', "发债主体评级预期", '发债主体评级发布时间',
                                '发债主体评级-季度1',
                                '发债主体评级-季度', '发债主体评级-年份', '债券唯一标识(年份-季度-证券代码)']
    data_firm_rating_col = ['债券唯一标识(年份-季度-证券代码)', "发债主体评级发布时间",
                            "发债主体评级类型", "发债主体评级信用等级", "发债主体评级评估机构",
                            "发债主体评级投资等级", "发债主体评级预期", "发债主体评级等级含义"]
    data_firm_rating_2 = data_firm_rating[data_firm_rating_col]
    single_code = list(set(data_firm_rating_2["债券唯一标识(年份-季度-证券代码)"]))
    single_company = list(set(data_firm_rating_2["发债主体评级评估机构"]))
    columns_rate_03_new = ["{}({})".format(y, x) for x in single_company for y in data_firm_rating_col[1:]]
    data_rate_03_new = pd.DataFrame(columns=["债券唯一标识(年份-季度-证券代码)"] + columns_rate_03_new)
    # ===07.1.开始按照债券代码和评估公司名称进行遍历循环,所有债券发行主体评级及等级含义输出(唯一标识码+分评估机构)===
    for indices_i, i in enumerate(single_code):
        time_mid = time.time()
        print("\r 主体评级整理-程序运行进度:{:.2%}<{:.4f}>小时, 预计还有{:.4f}小时...".format(
            (indices_i + 1) / len(single_code),
            (time_mid - time_start) / 3600,
            (time_mid - time_start) / 3600 / (indices_i + 1) * (len(single_code) - (indices_i + 1))),
            end="")
        temp_i_df = pd.DataFrame()
        temp_i_df["债券唯一标识(年份-季度-证券代码)"] = [i]
        for j in single_company:
            temp_df = data_firm_rating_2.loc[
                      (data_firm_rating_2["债券唯一标识(年份-季度-证券代码)"].isin([i])) & (
                          data_firm_rating_2["发债主体评级评估机构"].isin([j])),
                      :]
            if int(temp_df.shape[0]) >= 2:
                temp_df.drop_duplicates(subset=["债券唯一标识(年份-季度-证券代码)"], keep='first', inplace=True)
            temp_df.rename(columns=lambda x: x.replace(x, "{}({})".format(x, str(j))), inplace=True)
            temp_df.rename(columns={"债券唯一标识(年份-季度-证券代码)({})".format(str(j)): "债券唯一标识(年份-季度-证券代码)"}, inplace=True)
            temp_i_df = pd.merge(temp_i_df, temp_df, how="outer", on="债券唯一标识(年份-季度-证券代码)")
        data_rate_03_new = pd.concat([data_rate_03_new, temp_i_df], ignore_index=True)
    data_rate_03_new.to_csv(path_in + "Data_out\\" + '所有债券发行主体评级及等级含义输出(唯一标识码+分评估机构).csv', columns=["债券唯一标识(年份-季度-证券代码)"] + columns_rate_03_new,
                            encoding='utf-8-sig')

    # ===08.由于以上已经匹配了所有债券等级信息和债券发行主体评级信息,接下来需要将以上内容匹配到需要研究的债券中===
    # ===08.1.将债项评级信息匹配进去,记为'data_rating_for_bond_com'===
    data_rate_base = pd.read_excel(path_in + "Data_in\\" + '00.需要添加评级信息的债券基本信息.xlsx', sheet_name="Sheet1")  # 读取需要填补评级信息的债券基本数据
    data_rating_for_bond = copy.deepcopy(data_rate_base)  # 为避免修改原始债券基本数据
    data_rating_for_bond_com = data_rate_02_new.merge(data_rating_for_bond, how='right', on=["债券唯一标识(年份-季度-证券代码)"])
    # ===08.2.再将债券发行主体评级信息匹配进去,记为'data_rating_for_firm_com'===
    data_rating_for_bond_firm_com = data_rate_03_new.merge(data_rating_for_bond_com, how='right', on=["债券唯一标识(年份-季度-证券代码)"])
    data_rating_for_bond_firm_com.to_csv(path_in + "Data_out\\" + '00.需要添加评级信息的债券基本信息(分评估机构的债项+发债主体评级信息).csv',
                                         columns=["债券唯一标识(年份-季度-证券代码)"] + columns_rate_02_new + columns_rate_03_new, encoding='utf-8-sig')

    # ===09.保存为pickle文件,将变量设置及结果保存至pickle文件中===
    now_time_1 = datetime.datetime.now().strftime('%Y.%m.%d.%H')  # 记录当前的年、月、日、小时
    now_time_2 = datetime.datetime.now().strftime('%M')  # 记录当前的分钟
    save_name = now_time_1 + '：' + now_time_2 + '.保存的整理评级含义后的历史评级结果'
    path_out = path_in + "Data_out\\"
    save_obj = {'path_in': path_in, 'path_out': path_out,
                'data_rating_meaning': data_rating_meaning,
                'data_bond': data_bond,
                'data_firm': data_firm,
                "data_rate_03_new": data_rate_03_new,
                "data_rate_02_new": data_rate_02_new,
                "data_firm_rating": data_firm_rating,
                "data_bond_rating": data_bond_rating}
    save_with_pickle_ykp(path_out, save_name, save_obj)

    # 打印运行时间
    time_end_2 = time.time()
    print("\n 数据保存完毕,耗时{}分钟.".format((time_end_2 - time_start) / 60))


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    main()
