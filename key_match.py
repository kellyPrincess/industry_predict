# encoding = utf-8

import gc
import re
import os
import io
import time
import pandas as pd
from multiprocessing import Process


class MyClass(Process):
    os.chdir("E:\\lakala\\dwmc_jyfw_jtk")
    print(os.getcwd())

    first_match = "first_match.xlsx"
    df = pd.read_excel(first_match)
    len_match = len(df)

    dict_match = dict()
    for i in range(len_match):
        dict_match.update({
            df["key_word"][i]: [df["l2_industry"][i], df["l1_industry"][i], str(df["level"][i])]
        })

    def __init__(self, i):
        self.i = i
        super().__init__()

    def first_match(self, s):
        rst = "$$$"
        for k in self.dict_match:
            if k in s:
                l2_industry = self.dict_match[k][0]
                l1_industry = self.dict_match[k][1]
                level = self.dict_match[k][2]
                rst = "$".join([k, l2_industry, l1_industry, level])
                break
        return rst

    def run(self):
        fw = open('dwmc_jyfw_key_{}.txt'.format(self.i), "w", encoding="utf-8")
        fw_nm = open('dwmc_jyfw_Nkey_{}.txt'.format(self.i), "w", encoding="utf-8")
        cnt = 0
        with open('dwmc_jyfw_c_{}.txt'.format(self.i), "r", encoding="utf-8") as f:
            for line in f:
                lst = line.split('$')
                if lst[1]:
                    s = self.first_match(lst[1])
                    if "$$$" != s:
                        s = "$".join([lst[0], lst[1], s])
                        s = s.strip()
                        fw.write(s + "\n")
                    else:
                        fw_nm.write(line + "\n")
                cnt += 1
                if cnt % (5000 * 10) == 0:
                    fw.flush()
                    print("{} {} cnt ={}".format(self.i, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), cnt))
        fw.close()
        fw_nm.close()
        print('Run child process %s (%s)...' % (self.i, os.getpid()))


if __name__ == "__main__":
    pros = list()
    for i in range(12):
        p = MyClass(i)
        print('Process {} will start.'.format(i))
        p.start()
        pros.append(p)

    for p in pros:
        p.join()

    print("-- over --")
