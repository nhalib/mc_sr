from req_py_libraries import *
from be.functions_core_1 import *
from be.db_works import *
from _evaluate_portfolio import _evaluate_portfolio

# this module builds on the intermediate output from _prepare_rj_attn_matrices.py
class _lit_portfolios_1():

    def __init__(self):

        self.suid_fuid_list = "be/fund_suid_change_{0}.xlsx"
        self.stocks_usa = read_json(path="/scry_securities/scry/stocks_usa/exch_tick_cik.json")
        self.base_qtrs = base_keys()
        self.op_loc = "/scry_securities/scry_be/inst_inv/suid_buyers_sellers.xlsx"
        self.op_loc2 = "/scry_securities/scry_be/inst_inv/suid_buyers_sellers2.xlsx"
        self.portfolio_loc_base = "/scry_securities/scry_be/inst_inv/portfolio/"

        [self.suid_stocks_map] = self.suid_to_stocks_map()


    def suid_to_stocks_map(self):
        suid_stocks_map = {}
        for stock_row in self.stocks_usa:
            suid = stock_row["UID"]
            stock = stock_row["content"]["ticker"]
            suid_stocks_map[int(suid)] = stock
        return [suid_stocks_map]


    def list_all(self):
        path = "be/"
        folders = []

        for r, d, f in os.walk(path):
            for folder in f:
                folders.append(os.path.join(r, folder))

        files = []
        for f in folders:
            if f.find("xlsx") >= 0:
                files.append(f)
        return files


    def _process_suid_fuid(self):

        suid_dict = {}
        for file in self.list_all():

            full_str = file.split(".")[0]
            full_str_parts = full_str.split("_")

            qtr = full_str_parts[-1]

            tgt_df = pd.read_excel(file)

            for citer in tgt_df.index:

                suid = tgt_df.loc[citer,"suid"]

                net_position = tgt_df.loc[citer,"current_amount"] - tgt_df.loc[citer,"prev_amount"]

                if (suid,qtr) in suid_dict:pass
                else:suid_dict[(suid,qtr)] = {"increased_positions":0,"decreased_positions":0}

                if net_position >= 0:suid_dict[(suid, qtr)]["increased_positions"] += 1
                else:suid_dict[(suid, qtr)]["decreased_positions"] += 1


        suid_df = []
        for key in suid_dict:
            dict={
                "suid":key[0],
                "qtr":key[1],
                "increased_positions":suid_dict[key]["increased_positions"],
                "decreased_positions":suid_dict[key]["decreased_positions"]
            }
            suid_df.append(dict)
        suid_df = pd.DataFrame(suid_df)
        suid_df.to_excel(self.op_loc,index=False)


    # m2 and m3 measures from literature
    def _prepare_m23_measures(self):

        suid_df = pd.read_excel(self.op_loc)
        suid_df["m3_measure"] = suid_df["increased_positions"]/(suid_df["increased_positions"] + suid_df["decreased_positions"])

        qtr_suid_df = suid_df.groupby(by=["qtr"]).agg({"increased_positions":"sum","decreased_positions":"sum"})
        qtr_suid_df["p_val"] = qtr_suid_df["increased_positions"]/(qtr_suid_df["increased_positions"] + qtr_suid_df["decreased_positions"])
        suid_df = pd.merge(suid_df,qtr_suid_df,on=['qtr'],how='left')
        suid_df["m2_measure"] = suid_df["m3_measure"] - suid_df['p_val'] + np.random.rand(suid_df.shape[0])

        suid_df.to_excel(self.op_loc2, index=False)

        return [suid_df]


    def _prepare_portfolios(self,suid_df):

        portfolios = ["m2_portfolio.xlsx","m3_portfolio.xlsx"]
        measures = ['m2_measure','m3_measure']

        for jiter,portfolio_name in enumerate(portfolios):
            final_portfolio = []

            for qtr in suid_df['qtr'].unique():

                qtr_portfolio = []

                temp_df = suid_df[suid_df['qtr'] == qtr]
                temp_df = temp_df.sort_values(by=[measures[jiter]],ascending=False)
                temp_df = temp_df.reset_index(drop=True)

                temp_df = temp_df.head(100)

                for citer in temp_df.index:
                    suid = temp_df.loc[citer, "suid"]
                    try:ticker = self.suid_stocks_map[int(suid)]
                    except:ticker = "na"
                    qtr_portfolio.append({"ticker": ticker, "suid": suid, "qtr": qtr, "position": citer})

                final_portfolio += qtr_portfolio

            final_portfolio = pd.DataFrame(final_portfolio)

            final_portfolio.to_excel(self.portfolio_loc_base + portfolio_name, sheet_name="output", index=False)

    def _evaluate_portfolio(self):
        portfolio_names = ["m2_portfolio.xlsx", "m3_portfolio.xlsx"]
        tgt_positions = [20, 30, 40, 50]
        op_df = []
        for portfolio_name in portfolio_names:
            for portfolio_count in tgt_positions:
                print(portfolio_name, portfolio_count)

                k1 = _evaluate_portfolio(portfolio_name=portfolio_name, portfolio_size=portfolio_count)
                [tgt_df] = k1.setup_time_frames(k1.tgt_df)
                [tgt_df_grouped] = k1._find_nearest_price(tgt_df=tgt_df)

                # [sp_df] = k1._process_sp(tgt_df_grouped=tgt_df_grouped)

                cumul_gain = tgt_df_grouped["cumul_gain"].prod()
                mean_gain = tgt_df_grouped["gain"].mean()
                gain_std = tgt_df_grouped["gain"].std()

                temp_dict = {"portfolio_size": portfolio_count,
                             "cumul_gain": cumul_gain,
                             "mean_gain": mean_gain,
                             "std_gain": gain_std}

                op_df.append(temp_dict)

        op_df = pd.DataFrame(op_df)
        op_df.to_excel("/scry_securities/scry_be/inst_inv/portfolio/model_performance_lit_models.xlsx")


if __name__ == "__main__":
    l1 = _lit_portfolios_1()
    #l1._process_suid_fuid()
    [suid_df] = l1._prepare_m23_measures()
    l1._prepare_portfolios(suid_df=suid_df)
    l1._evaluate_portfolio()