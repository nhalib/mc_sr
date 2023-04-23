from req_py_libraries import *


class _portfolio_turnover():

    def __init__(self):

        self.alphas = ["00","10","15","20","25","30","50"]
        self.portfolio_loc = "/scry_securities/scry_be/inst_inv/portfolio/portfolio876-{0}.xlsx"
        self.portfolio_size = 50

    def _process_portfolio(self):

        for alpha in self.alphas:
            diff_train = []

            tgt_portfolio = pd.read_excel(self.portfolio_loc.format(alpha))

            qtrs = tgt_portfolio["qtr"].unique().tolist()
            qtrs = sorted(qtrs)

            for citer,qtr in enumerate(qtrs):

                print(tgt_portfolio.head(5))

                f1 = (tgt_portfolio["qtr"] == qtr)
                f2 = (tgt_portfolio["position"] <= self.portfolio_size)
                sub_portfolio = tgt_portfolio[f1 & f2]
                new_unique_stocks = sub_portfolio["ticker"].unique().tolist()

                if citer > 0:
                    diff_stocks = abs(new_unique_stocks - old_unique_stocks)
                    diff_train.append(diff_stocks)

                old_unique_stocks = new_unique_stocks

            print(alpha,np.mean(diff_train)/self.portfolio_size)

if __name__ == "__main__":
    p1 = _portfolio_turnover()
    p1._process_portfolio()