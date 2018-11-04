# encoding: utf-8 

import pandas as pd
import os
import time
import glob
import json
import multiprocessing
import numpy as np


def perfect_all_key():
    df = pd.read_excel(r"keyword_industry_0920.xlsx")
    df["k1"] = None
    df["k2"] = None
    df["k3"] = None
    df["k4"] = None
    df["k5"] = None

    cnt = 0
    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            cnt += 1
            s = row["key_word"]
            if len(s) in [2, 3]:
                df.iloc[idx, 4] = s
            elif len(s) == 4:
                df.iloc[idx, 4] = s[:2]
                df.iloc[idx, 5] = s[2:]
            elif len(s) == 5:
                df.iloc[idx, 4] = s[:3]
                df.iloc[idx, 5] = s[3:]
            elif len(s) == 6:
                df.iloc[idx, 4] = s[:2]
                df.iloc[idx, 5] = s[2:4]
                df.iloc[idx, 6] = s[4:]
            else:
                df.iloc[idx, 4] = s
    print("cnt = {}".format(cnt))
    df.to_excel("keyword_industry_0920_all.xlsx", index=False)


class MyProcess(multiprocessing.Process):
    def __init__(self, file_id, process_id, kword_dict, source_file_path):
        multiprocessing.Process.__init__(self)
        self.file_id = file_id
        self.process_id = process_id
        self.kword_dict = kword_dict
        self.source_file_path = source_file_path

    def run(self):
        print("{} {}, need to process cnt = {}".format(self.file_id, self.process_id, len(self.kword_dict)))
        cnt = 0
        with open(self.source_file_path, "r", encoding="utf-8") as f:
            for line in f:
                cnt += 1
                lst = line.split('$')
                if lst[1]:
                    for k in self.kword_dict:
                        if k in lst[1]:
                            self.kword_dict[k] += 1
                            break
                if cnt % (5000 * 100) == 0:
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    print("{} {} {} cnt ={}".format(self.file_id, self.process_id, ts, cnt))
        filename = "E:\\lakala\\2018-09-26\\freq_{}_{}.csv".format(self.file_id, self.process_id)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(json.dumps(self.kword_dict))
        print("{} {} tmp over".format(self.file_id, self.process_id))


def cal_one_file_freq(kword_dict, source_file_path, file_id):
    kwords_lst = list(kword_dict.keys())
    # 6个线程并行执行
    lst = list(range(0, len(kwords_lst), int(len(kwords_lst)/10.0))) + [len(kwords_lst)]
    lst = sorted(list(set(lst)))

    print("lst = {}\n".format(json.dumps(lst)))
    process_pools = []
    for j in list(range(len(lst)))[1:]:
        begin = lst[j-1]
        end = lst[j]
        tmp_dict = {k: kword_dict[k] for k in kword_dict if k in kwords_lst[begin: end]}
        process = MyProcess(file_id, j, tmp_dict.copy(), source_file_path)
        process.start()
        process_pools.append(process)

    for p in process_pools:
        p.join()
    print("final process over ...")

    files = glob.glob("E:\\lakala\\2018-09-26\\freq_{}_*.csv".format(file_id))
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            tmp_dict = json.loads(f.readline().strip())
        for k in tmp_dict:
            kword_dict[k] += tmp_dict[k]

    file_path = "E:\\lakala\\2018-09-26\\freq_{}.csv".format(file_id)
    with open(file_path, "w", encoding="utf-8") as f:
        s = json.dumps(kword_dict)
        f.write(s)
    print(" -- {} -- over".format(file_id))


def stat_freq():
    df = pd.read_excel("E:\\lakala\\2018-09-26\\keyword_industry_0920.xlsx")
    kword_dict = {}  # 预先定义一个全局的词频字典
    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            k = row["key_word"].strip()
            kword_dict[k] = 0

    # 匹配11个文件，留一个文件作为测试
    for i in range(11):
        source_file_path = "E:\\lakala\\dwmc_jyfw_jtk\\{}".format("dwmc_jyfw_c_{}.txt".format(i))
        kword_dict2 = kword_dict.copy()  # 复制字典，不修改原始的全局字典
        cal_one_file_freq(kword_dict2, source_file_path, i)

    rst_lst = glob.glob("E:\\lakala\\2018-09-26\\freq_*.csv")
    for path in rst_lst:
        with open(path, "r") as f:
            tmp_dict = json.loads(f.readline().strip())
        for k in tmp_dict:
            kword_dict[k] += tmp_dict[k]

    # 汇总全部的词频结果
    with open("E:\\lakala\\2018-09-26\\final_freq.csv", "w", encoding="utf-8") as f:
        for k, v in sorted(kword_dict.items(), key=lambda x: x[1], reverse=True):
            s = "{}, {}".format(k, v)
            f.write(s + "\n")


def cal_prior_prob():
    """
    计算先验概率
    note:计算频率时候，以非重复的单位数目作为基准!
    """
    # 原始的keyword的频率存为字典
    kword_freq = dict()
    with open("data\\final_freq.csv", "r", encoding="utf-8") as f:
        for line in f:
            lst = [i.strip() for i in line.strip().split(",")]
            kword_freq[lst[0]] = int(lst[1])

    # 获取全部的非重复l2和l1（合并起来），初始化为字典
    df = pd.read_excel("data\\keyword_industry_0920_all.xlsx")
    cnt = 0
    prior_keys = []
    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            k = "{} $ {}".format(row["l2_industry"], row["l1_industry"])
            prior_keys.append(k)
    prior_keys = set(prior_keys)
    prior_dict = {i:0 for i in prior_keys}  # 列表转为字典

    # 统计l2和l1的频率
    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            k = "{} $ {}".format(row["l2_industry"], row["l1_industry"])
            w = row["key_word"]
            prior_dict[k] += kword_freq[w]

    cnt = 40000 * 1000  # 训练集中的样本（单位）总数，整个需要真实统计，这里是个测试
    fw_cnt = open("data\\prior_cnt.csv", "w", encoding="utf-8")
    fw_freq = open("data\\prior_prob.csv", "w", encoding="utf-8")
    for i in sorted(prior_dict.items(), key=lambda x: x[1], reverse=True):
        fw_cnt.write("{}, {}\n".format(i[0], i[1]))
        fw_freq.write("{}, {}\n".format(i[0], 1.0*i[1]/cnt))
    fw_cnt.close()
    fw_freq.close()


def cal_cond_prob():
    """
    计算条件概率
    """
    raw_kword_freq = dict()
    with open("data\\final_freq.csv", "r", encoding="utf-8") as f:
        for line in f:
            lst = [i.strip() for i in line.strip().split(",")]
            raw_kword_freq[lst[0]] = int(lst[1])

    with open("data\\prior_cnt.csv", "r", encoding="utf-8") as f:
        lst = [line.split(",") for line in f.readlines()]
        prior_cnt_dict = {i[0].strip(): int(i[1].strip()) for i in lst}

    df = pd.read_excel("data\\keyword_industry_0920_all.xlsx")
    label_kword_dict = {k: [] for k in prior_cnt_dict.keys()}
    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            label = "{} $ {}".format(row["l2_industry"], row["l1_industry"])
            lst = [str(row["k1"]), str(row["k2"]), str(row["k3"]), str(row["k4"]), str(row["k5"])]
            lst = list(filter(lambda x: x != "nan", lst))
            label_kword_dict[label].extend(lst)

    for label in label_kword_dict:
        label_kword_dict[label] = list(set(label_kword_dict[label]))

    post_prob_dict = dict()
    for label in label_kword_dict:
        post_prob_dict[label] = dict()
        for kw in label_kword_dict[label]:
            post_prob_dict[label][kw] = 0

    for idx, row in df.iterrows():
        if row["level"] in [1, 2]:
            label = "{} $ {}".format(row["l2_industry"], row["l1_industry"])
            lst = [str(row["k1"]), str(row["k2"]), str(row["k3"]), str(row["k4"]), str(row["k5"])]
            lst = list(filter(lambda x: x != "nan", lst))
            for w in lst:
                post_prob_dict[label][w] += raw_kword_freq[row["key_word"]]

    with open("data\\cond_prob.csv", "w", encoding="utf-8") as f:
        for label in post_prob_dict:
            for kw in post_prob_dict[label]:
                if prior_cnt_dict[label] > 0:
                    s = "{},{},{}".format(label, kw, 1.0*post_prob_dict[label][kw]/prior_cnt_dict[label])
                    f.write(s + "\n")


def inference(words):
    cond_prob_dict = dict()
    with open("data\\cond_prob.csv", "r", encoding="utf-8") as f:
        for line in f:
            label = line.split(",")[0]
            word = line.split(",")[1]
            if label not in cond_prob_dict:
                cond_prob_dict[label] = dict()
                cond_prob_dict[label][word] = float(line.split(",")[2])
            else:
                cond_prob_dict[label][word] = float(line.split(",")[2])

    print("cond_prob_dict")
    print(cond_prob_dict)

    with open("data\\prior_prob.csv", "r", encoding="utf-8") as f:
        lst = [line.split(",") for line in f.readlines()]
        prior_prob_dict = {i[0].strip(): float(i[1].strip()) for i in lst}

    print("\nprior_prob_dict")
    print(prior_prob_dict)
    print("")

    rst_cnt = dict()
    for label in prior_prob_dict:
        prob = 1.0 * prior_prob_dict[label]
        if label in cond_prob_dict:
            cnt = 0
            for w in words:
                if w in cond_prob_dict[label]:
                    cnt += 1
                    rst_c
                    nt[label] = cnt
    if len(rst_cnt) > 0:
        max_v = max(rst_cnt.values())
        if max_v > 1:
            # 存在多个关键词匹配上条件概率，则选择先验概率最大的那个类别
            rst = {k: prior_prob_dict[k] for k in rst_cnt if rst_cnt[k] == max_v}
            return max(rst, key=rst.get)
        else:
            # 多个候选词只出现一次，则选择先验概率最大的类
            rst = {k: prior_prob_dict[k] for k in rst_cnt}
            return max(rst, key=rst.get)
    else:
        return "unknown"


def main():
    # perfect_all_key()
    # cal_prior_prob()
    # print("\ncal_cond_prob")
    # cal_cond_prob()

    rst = inference(["", ""])
    print(rst)


if __name__ == '__main__':
    main()
    print("-- final over --")

