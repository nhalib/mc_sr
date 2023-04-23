import pandas as pd

from be.functions_core_2 import *
db_loc = "/scry_securities/scry_be/inst_inv/dbs/"
funds_loc = "/scry_securities/scry/stocks_usa/funds.json"


def position_change_central(fuid_sublist,cur_qtr,prev_qtr,base_qtrs):
    for fuid in fuid_sublist:
        try:
            fund_validate_stat = validate_fund(fuid=fuid, qtr=cur_qtr, all_qtrs=base_qtrs)
            if fund_validate_stat :
                position_change(fuid=fuid,cur_qtr=cur_qtr,prev_qtr=prev_qtr)
            else:
                pass

        except Exception as e:
            pass


def mp_position_change(fuid_sublists,cur_qtr,prev_qtr,base_qtrs):

    mps = []
    for fuid_sublist in fuid_sublists:
        p1 = multiprocessing.Process(target=position_change_central, args=(fuid_sublist,cur_qtr,prev_qtr,base_qtrs,))
        mps.append(p1)

    for mp in mps: mp.start()
    for mp in mps: mp.join()






