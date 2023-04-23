import os
import pandas as pd
import h5py
import ast
from fiscalyear import *
from datetime import datetime, timedelta
import datetime as dt
import numpy as np
import time
import math
import pandas as pd
from sklearn.cluster import KMeans
import json
import matplotlib.pyplot as plt
import pickle
import multiprocessing
import shutil
from random import sample
import csv

from numpy import dstack
from pandas import read_csv

import tulipy as ti

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

max_suid = 9000
code_base_parent = "/scry_securities/"

np.random.seed(0)

# dump data to json file
def dump_json(path,data,add_code_base_parent=True):
    if add_code_base_parent:path2 = ("").join(x for x in [code_base_parent,path]).replace("//","/")
    try:
        with open(path2, 'w') as outfile:
            json.dump(data, outfile)
    except:
        with open(path, 'w') as outfile:
            json.dump(data, outfile)


#load json file
def read_json(path,add_code_base_parent=True):
    if add_code_base_parent:path2 = ("").join(x for x in [code_base_parent, path]).replace("//","/")
    try:
        with open(path2) as json_data:
            data = json.load(json_data)
            json_data.close()
    except:
        try:
            with open(path) as json_data:
                data = json.load(json_data)
                json_data.close()
        except:
            data = {}
    return data


def get_nearest_after(keys, tgt_val):
    tgt_df = pd.DataFrame([], columns=["date", "tgt_date"])
    items = [datetime.strptime(x, "%Y-%m-%d").date() for x in keys]
    pivot = datetime.strptime(tgt_val, "%Y-%m-%d").date()
    tgt_df["date"] = items
    tgt_df["tgt_date"] = pivot
    tgt_df["delta"] = (tgt_df["date"] - tgt_df["tgt_date"])/np.timedelta64(1,'D')
    tgt_df = tgt_df[tgt_df["delta"] >= 0]
    tgt_df = tgt_df.sort_values(by=["delta"], ascending=True)
    tgt_df = tgt_df.reset_index(drop=True)
    return tgt_df.loc[0,"date"]


def get_nearest_before(keys,tgt_val):
    tgt_df = pd.DataFrame([], columns=["date", "tgt_date"])
    items = [datetime.strptime(x, "%Y-%m-%d").date() for x in keys]
    pivot = datetime.strptime(tgt_val, "%Y-%m-%d").date()
    tgt_df["date"] = items
    tgt_df["tgt_date"] = pivot
    tgt_df["delta"] = (tgt_df["tgt_date"] - tgt_df["date"])/np.timedelta64(1,'D')
    tgt_df = tgt_df[tgt_df["delta"] >= 0]
    tgt_df = tgt_df.sort_values(by=["delta"], ascending=True)
    tgt_df = tgt_df.reset_index(drop=True)
    return tgt_df.loc[0,"date"]


def save_pickle(fname,data):
    with open(fname+'.pickle', 'wb') as f:
        pickle.dump(data, f)

def read_pickle(fname):
    with open(fname+'.pickle') as f:
        loaded_obj = pickle.load(f)
    return loaded_obj


def divide_chunks(l, n):
    # looping till length l
    if n >= 1:
        for i in range(0, len(l), n):
            yield l[i:i + n]
    elif n < 1:
        yield l


def l3(ticker,suid):
    suid_data = read_json(path="/scry/stocks_usa/exch_tick_cik.json")
    ele = 0
    for row in suid_data:
                if int(row['UID']) == int(suid) or row['content']['ticker'].find(ticker) >= 0:
                    #print(row['UID'],row['content']['ticker'],row['content']["sector"])
                    ele = row["content"]["ticker"]
    return ele