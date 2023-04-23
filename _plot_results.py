import pandas as pd

from req_py_libraries import *


class _analyze_hfi():

    def __init__(self):

        self.model_data = pd.read_excel("/scry_securities/scry_be/inst_inv/portfolio/model_performance.xlsx",sheet_name="eurekahedge_na_index")

        self.hfri_index = pd.read_excel("/scry_securities/scry_be/inst_inv/portfolio/hfri_equity.xlsx")


    def _evaluate_eureka_na(self):

        self.model_data["Group"] = self.model_data["Group"].astype(int)
        self.grouped_df = self.model_data.groupby(by=["Group"]).agg({"gain":"sum"})
        self.grouped_df["cumul_gain"] = self.grouped_df["gain"]/100 + 1

        print(self.grouped_df["cumul_gain"].prod(),self.grouped_df["gain"].std())

    def _evaluate_hfri_index(self):

        self.hfri_index["gain"] = pd.to_numeric(self.hfri_index["gain"])
        self.hfri_index["Group"] = self.hfri_index["Group"].astype(int)
        self.grouped_df = self.hfri_index.groupby(by=["Group"]).agg({"gain": "sum"})
        self.grouped_df["cumul_gain"] = self.grouped_df["gain"] / 100 + 1

        print(self.grouped_df["cumul_gain"].prod(), self.grouped_df["gain"].std())


if __name__ == "__main__":
    f1 = _analyze_hfi()
    f1._evaluate_hfri_index()
