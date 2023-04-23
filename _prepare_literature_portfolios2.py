import pandas as pd

from req_py_libraries import *

hloc = "/scry_securities/scry/securities/"


class _lit_portfolios_2():

    def __init__(self):
        self.fuid_pool_loc = "/scry_securities/scry/inst_inv/"


    def list_all(self):
        path = self.fuid_pool_loc
        folders = []

        for r, d, f in os.walk(path):
            for folder in f:
                folders.append(os.path.join(r, folder))

        files = []
        for f in folders: files.append(f)
        return files


    def _process_funds(self):

        fund_df = []
        fund_df2 = []
        year_dict = {}

        for file in self.list_all():

            print(file)

            fund_data = read_json(path=file)
            fund_dict = []

            file_parts = file.split("/")
            fuid = file_parts[-1].split(".")[0]

            for key in sorted(fund_data): # key is the actual filing date

                for suid_key in fund_data[key]:
                    try:

                        suid = suid_key.replace("suid_","")
                        start_price = float(fund_data[key][suid_key]["start_val"])
                        shares = int(fund_data[key][suid_key]["shares"])
                        market_value = start_price * shares
                        qtr_date = fund_data[key][suid_key]["report_date"]
                        qtr_year = qtr_date.split('-')[0]

                        dict = {"suid":suid,"price":start_price,"shares":shares,"year":qtr_year,"report_date":qtr_date,"market_value":market_value}
                        fund_dict.append(dict)

                    except:pass


            fund_df = pd.DataFrame(fund_dict)
            if len(fund_df.index) > 0:

                fund_df.report_date = pd.to_datetime(fund_df.report_date)
                fund_df['quarter'] = pd.PeriodIndex(fund_df.report_date, freq='Q')

                qiter = 0
                for qtr,qtr_fund_df in fund_df.groupby(by=["quarter"]):

                    if qiter > 0:
                        cur_stocks = qtr_fund_df["suid"].values.tolist()
                        shares_held_market_value = qtr_fund_df["market_value"].sum()
                        prev_stocks = prev_qtr_df["suid"].values.tolist()

                        positions_bought = list(set(cur_stocks) - set(prev_stocks))
                        positions_sold = list(set(prev_stocks) - set(cur_stocks))
                        positions_held = set(cur_stocks).intersection(set(prev_stocks))

                        temp_dict = {"qtr":qtr,
                                     "positions_bought":len(positions_bought),
                                     "positions_sold":len(positions_sold),
                                     "market_value":shares_held_market_value,
                                     "positions_held":len(list(positions_held)),
                                     "fuid":fuid}

                        fund_df2.append(temp_dict)

                    prev_qtr_df = qtr_fund_df
                    qiter += 1


        fund_df2 = pd.DataFrame(fund_df2)
        fund_df2.to_excel('be/fund_op2.xlsx')


    def _consolidate_funds(self):

        fund_df2 = pd.read_excel('be/fund_op2.xlsx')
        fund_df2 = fund_df2.groupby(by=['qtr']).agg({'positions_bought':"mean",'positions_sold':'mean','market_value':'sum','positions_held':'mean','fuid':"count"})
        fund_df2.to_excel('be/fund_op3.xlsx')



if __name__ == "__main__":

    l1 = _lit_portfolios_2()
    l1._consolidate_funds()