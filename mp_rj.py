from be.functions_core_2 import *
db_loc = "/scry_securities/scry_be/inst_inv/dbs/"
funds_loc = "/scry_securities/scry/stocks_usa/funds.json"
mat_loc = "/scry_securities/scry_be/inst_inv/matrices/"

def mp_rj_weights(cur_qtr):
    fuid_keys = get_written_funds()
    fuid_keys = divide_chunks(fuid_keys,int(len(fuid_keys)/5))

    mps = []
    iter = 0
    diff_mats = []

    for fuid_sublist in fuid_keys:
        p1 = multiprocessing.Process(target=rj_attn, args=(fuid_sublist,cur_qtr,iter,))
        mps.append(p1)
        diff_mats.append("qtr_"+str(cur_qtr).replace("-","_")+"_"+str(iter)+".h5")
        iter += 1

    for mp in mps: mp.start()
    for mp in mps: mp.join()

    if len(diff_mats) > 0:
        hf1 = h5py.File(mat_loc+diff_mats[0], "r")
        mat1 = hf1.get("rj_matrix")

        rslt = np.array(mat1)
        os.remove(mat_loc+diff_mats[0])

        for iter in range(1,len(diff_mats)):

            hf1 = h5py.File(mat_loc+diff_mats[iter], "r")
            mat2 = np.array(hf1["rj_matrix"])

            rslt = rslt.__add__(mat2)
            os.remove(mat_loc+diff_mats[iter])

        hf = h5py.File(mat_loc + "rj_qtr_"+str(cur_qtr).replace("-","_")+".h5", 'w')
        hf.create_dataset('rj_matrix', data=rslt)
        hf.close()

