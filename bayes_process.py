# encoding: utf-8 

import pandas as pd
import os
import time
import glob
import json
import multiprocessing

os.chdir("E:\\lakala\\2018-09-26\\")


def crt_key():
    df = pd.read_excel(r"E:\lakala\2018-09-26\keyword_industry_0920.xlsx")
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
    df.to_excel("ttt.xlsx", index=False)


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


def main():
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


if __name__ == '__main__':
    main()
    print("-- final over --")
