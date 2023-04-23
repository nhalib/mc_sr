from be.position_change_core import *
from be.mp_attention_diff import mp_attention_diff
from be.mp_rj import mp_rj_weights

fuid_suid_change_loc = "be/fund_suid_change.xlsx"

class rj_attn_mats():

    def cumul_prepare_matrices(self):
            base_qtrs = base_keys()
            print(base_qtrs)

            funds = read_json(path=funds_loc)
            iter = 0

            if True:
                for qtr in base_qtrs:
                    if iter >= 1:

                            fund_eval = True
                            if fund_eval:
                                fuid_list = []
                                for fund in funds:fuid_list.append(fund["UID"])
                                fuid_sublists = list(divide_chunks(fuid_list,1000))
                                mp_position_change(fuid_sublists=fuid_sublists,cur_qtr=qtr,prev_qtr=base_qtrs[iter-1],base_qtrs=base_qtrs,)

                            diffusion_eval = True
                            if diffusion_eval:
                                # this routine below computes the 'attention' diffused between stocks
                                mp_attention_diff(cur_qtr=qtr,prev_qtr=base_qtrs[iter-1])

                            rj_eval = True
                            if rj_eval:
                                mp_rj_weights(cur_qtr=qtr)

                    iter += 1
                    print(qtr)



if __name__ == "__main__":

    df = pd.DataFrame([])
    df.to_excel(fuid_suid_change_loc,index=False)

    r1 = rj_attn_mats()
    r1.cumul_prepare_matrices()