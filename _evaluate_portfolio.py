import pandas as pd

from req_py_libraries import *
from be.position_change_core import *
# use a dataframe structure to evaluate portfolio worth

class _evaluate_portfolio():

    def __init__(self, portfolio_name, portfolio_size):

        self.portfolio_loc = "/scry_securities/scry_be/inst_inv/portfolio/{0}".format(portfolio_name)
        self.tgt_df = pd.read_excel(self.portfolio_loc)

        self.base_qtrs = base_keys()
        self.price_evaluation_ticker = "nan"
        self.eod_data_loc = "/scry_securities/scry/securities/{0}/eod.json"
        self.sp_data = read_json("/scry_securities/scry/global/indices/s_p.json")

        self.delta = 47
        self.tgt_positions_ll = portfolio_size

        self.base_constant = self.tgt_positions_ll + 1


    def setup_time_frames(self,tgt_df):

        tgt_df['qtr'] = pd.to_datetime(tgt_df['qtr'],utc=False)
        tgt_df["buy_date"] = tgt_df["qtr"] + timedelta(days=self.delta)
        tgt_df = tgt_df[(tgt_df["position"] <= self.tgt_positions_ll)]

        tgt_df_grouped1 = tgt_df.groupby(by=["qtr"],as_index=False).agg({"position":"count"})
        tgt_df_grouped1 = tgt_df_grouped1.rename(columns={"position":"portfolio_count"})

        tgt_df_grouped = tgt_df_grouped1
        tgt_df_grouped["sell_date"] = str(datetime.now().date())

        tgt_df_grouped = tgt_df_grouped.sort_values(by=["qtr"],ascending=True)
        tgt_df_grouped = tgt_df_grouped.reset_index(drop=True)

        for citer in tgt_df_grouped.index:
            if citer < len(tgt_df_grouped.index)-2:
                #print(tgt_df_grouped.loc[citer,"qtr"] + timedelta(days=self.delta))
                tgt_df_grouped.loc[citer,"sell_date"] = tgt_df_grouped.loc[citer+1,"qtr"] + timedelta(days=self.delta)
        tgt_df = pd.merge(tgt_df,tgt_df_grouped,on=["qtr"])

        return [tgt_df]


    def _find_nearest_price(self,tgt_df):

        tgt_df["start_price"] = 0
        tgt_df["end_price"] = 0
        tgt_df["weight"] = 0

        for citer in tgt_df.index:
            ticker = tgt_df.loc[citer,"ticker"]
            suid = tgt_df.loc[citer,"suid"]
            start_date = tgt_df.loc[citer,"buy_date"]
            end_date = tgt_df.loc[citer,"sell_date"]

            if ticker == self.price_evaluation_ticker:pass
            else:
                self.eod_data = read_json(path=self.eod_data_loc.format(suid))
                self.eod_keys = sorted(self.eod_data)
                self.price_evaluation_ticker = ticker

            try:
                start_date = get_nearest_after(keys=self.eod_keys, tgt_val=str(start_date.date()))
                end_date = get_nearest_after(keys=self.eod_keys, tgt_val=str(end_date.date()))
                start_price = self.eod_data[str(start_date)]["adj_open"]
                end_price = self.eod_data[str(end_date)]["adj_close"]
            except Exception as e:
                start_price = 0
                end_price = 0

            tgt_df.loc[citer,"weight"] = np.exp(-0.01*(tgt_df.loc[citer,"position"])*0)
            tgt_df.loc[citer,"start_price"] = float(start_price)
            tgt_df.loc[citer,"end_price"] = float(end_price)

        tgt_df_grouped1 = tgt_df.groupby(by=["qtr"], as_index=False).agg({"weight":"sum"})
        tgt_df_grouped1 = tgt_df_grouped1.rename(columns={"weight":"cumul_weight"})
        tgt_df["gain"] = ((tgt_df["end_price"]/tgt_df["start_price"]) - 1) - 0.0002
        tgt_df = pd.merge(tgt_df,tgt_df_grouped1,on=["qtr"],how="left")

        tgt_df["gain"] = tgt_df["gain"] * tgt_df["weight"] / tgt_df["cumul_weight"]
        tgt_df["gain"] = tgt_df["gain"].fillna(0)
        #print(tgt_df.head(5))
        #print(tgt_df[["ticker","buy_date","sell_date","start_price","end_price","gain"]])
        tgt_df_grouped = tgt_df.groupby(by=["qtr"],as_index=False).agg({"gain":"sum",})
        tgt_df_grouped["cumul_gain"] = tgt_df_grouped["gain"] + 1

        return [tgt_df_grouped]


    def _process_sp(self,tgt_df_grouped):

        tgt_keys = sorted(self.sp_data)

        df = []
        for citer in tgt_df_grouped.index:

            if citer < len(tgt_df_grouped.index)-2:
                qtr = tgt_df_grouped.loc[citer,"qtr"]
                qtr_next = tgt_df_grouped.loc[citer+1,"qtr"]

                start_date_ideal = qtr + timedelta(days=self.delta)
                end_date_ideal = qtr_next + timedelta(days=self.delta)

                try:
                    start_date = get_nearest_after(keys=tgt_keys,tgt_val=str(start_date_ideal.date()))
                    end_date = get_nearest_after(keys=tgt_keys,tgt_val=str(end_date_ideal.date()))
                    start_price = float(self.sp_data[str(start_date)])
                    end_price = float(self.sp_data[str(end_date)])
                except Exception as e:
                    start_price = 0
                    end_price = 0


                gain = (end_price/start_price) -1
                df.append({"qtr":qtr,"gain":gain})

        df = pd.DataFrame(df)
        df["cumul_gain"] = df["gain"] + 1


        return [df]


if __name__ == "__main__":
    portfolio_names = ["portfolio876-00.xlsx","portfolio876-10.xlsx","portfolio876-15.xlsx","portfolio876-20.xlsx","portfolio876-25.xlsx","portfolio876-30.xlsx","portfolio876-50.xlsx"]
    tgt_positions = [20,30,40,50]
    merge_factors = [0,0.1,0.15,0.2,0.25,0.3,0.5]
    op_df = []
    for merge_factor,portfolio_name in zip(merge_factors,portfolio_names):
        for portfolio_count in tgt_positions:

            print(merge_factor,portfolio_name,portfolio_count)

            k1 = _evaluate_portfolio(portfolio_name=portfolio_name,portfolio_size=portfolio_count)
            [tgt_df] = k1.setup_time_frames(k1.tgt_df)
            [tgt_df_grouped] = k1._find_nearest_price(tgt_df=tgt_df)

            #[sp_df] = k1._process_sp(tgt_df_grouped=tgt_df_grouped)

            cumul_gain = tgt_df_grouped["cumul_gain"].prod()
            mean_gain = tgt_df_grouped["gain"].mean()
            gain_std = tgt_df_grouped["gain"].std()

            temp_dict = {"portfolio_size":portfolio_count,
                         "merge_factor":merge_factor,
                         "cumul_gain":cumul_gain,
                         "mean_gain":mean_gain,
                         "std_gain":gain_std}

            op_df.append(temp_dict)

    op_df = pd.DataFrame(op_df)
    op_df.to_excel("/scry_securities/scry_be/inst_inv/portfolio/model_performance_3.xlsx")