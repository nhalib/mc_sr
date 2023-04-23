from req_py_libraries import *
from be.position_change_core import *

class prepare_portfolio():

    def __init__(self,merge_factor,portfolio_name):

        self.stocks_usa = read_json(path="/scry_securities/scry/stocks_usa/exch_tick_cik.json")
        self.merge_factor = merge_factor
        self.portfolio_loc_base = "/scry_securities/scry_be/inst_inv/portfolio/"
        self.mat_loc_base = "/scry_securities/scry_be/inst_inv/matrices/"
        self.base_qtrs = base_keys()
        [self.suid_stocks_map] = self.suid_to_stocks_map()
        self.portfolio_name = portfolio_name

        print(self.merge_factor,self.portfolio_name)

    def _get_qtr_matrices(self,cur_qtr):

        hf = h5py.File(mat_loc + "rj_qtr_"+str(cur_qtr).replace("-","_")+".h5", "r")
        rj_matrix = hf.get("rj_matrix")
        hf = h5py.File(mat_loc + "diffusion_qtr_" + str(cur_qtr).replace("-", "_") + ".h5", "r")
        diff_matrix = hf.get("diffusion_matrix")
        return [rj_matrix,diff_matrix]

    def _build_transition_mats(self,rj_matrix,diff_matrix):

        rj_matrix = np.array(rj_matrix)
        diff_matrix = np.array(diff_matrix)

        for suid in range(0,max_suid):
            rj_matrix[suid,:] = rj_matrix[suid,:]/np.sum(rj_matrix[suid,:])
            if np.sum(diff_matrix[suid,:]) > 0:
                diff_matrix[suid, :] = diff_matrix[suid, :] / np.sum(diff_matrix[suid,:])

        transition_matrix = (1-self.merge_factor)*rj_matrix + self.merge_factor*diff_matrix

        return [transition_matrix]


    def suid_to_stocks_map(self):
        suid_stocks_map = {}
        for stock_row in self.stocks_usa:
            suid = stock_row["UID"]
            stock = stock_row["content"]["ticker"]
            suid_stocks_map[int(suid)] = stock
        return [suid_stocks_map]


    def run_transitions(self,transition_matrix,cur_qtr):
        qtr_portfolio = []

        transition_matrix = transition_matrix.transpose()
        cur_mat = np.ones(shape=(max_suid,1))
        cur_mat = cur_mat/max_suid

        citer = 0
        while citer < 300:
            cur_mat = np.matmul(transition_matrix,cur_mat)
            citer += 1

        tgt_df = []
        for tgt_index,ele in enumerate(cur_mat):
            tgt_df.append({"suid":tgt_index,"prob":ele})
        tgt_df = pd.DataFrame(tgt_df)
        tgt_df = tgt_df.sort_values(by=["prob"],ascending=False)
        tgt_df = tgt_df.reset_index(drop=True)

        tgt_df = tgt_df.head(100)
        for citer in tgt_df.index:
            suid = tgt_df.loc[citer,"suid"]
            try:ticker = self.suid_stocks_map[int(suid)]
            except:ticker = "na"
            qtr_portfolio.append({"ticker":ticker,"suid":suid,"qtr":cur_qtr,"position":citer})

        return [qtr_portfolio]


    def _build_portfolio(self):
        tgt_portfolio = []
        for iter in range(1,len(self.base_qtrs)):
            tgt_qtr = self.base_qtrs[iter]
            [rj_matrix, diff_matrix] = self._get_qtr_matrices(cur_qtr=tgt_qtr)
            [transition_matrix] = self._build_transition_mats(rj_matrix=rj_matrix, diff_matrix=diff_matrix)
            [qtr_portfolio] = self.run_transitions(transition_matrix=transition_matrix,cur_qtr=tgt_qtr)
            tgt_portfolio = tgt_portfolio + qtr_portfolio

        tgt_portfolio = pd.DataFrame(tgt_portfolio)
        tgt_portfolio.to_excel(self.portfolio_loc_base+self.portfolio_name,sheet_name="output",index=False)


if __name__ == "__main__":

    merge_factors = [0.0,0.1,0.15,0.2,0.25,0.3,0.5]
    portfolio_names = ["portfolio876-00.xlsx","portfolio876-10.xlsx","portfolio876-15.xlsx","portfolio876-20.xlsx","portfolio876-25.xlsx","portfolio876-30.xlsx","portfolio876-50.xlsx"]

    for jiter,merge_factor in enumerate(merge_factors):
        d1 = prepare_portfolio(merge_factor=merge_factor,portfolio_name=portfolio_names[jiter])
        d1._build_portfolio()




