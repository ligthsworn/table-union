from helper import nCr, nPr

def cal_set_unionability(dataset1, dataset2):
    set1 = set(dataset1)
    set2 = set(dataset2)
    inter_sec = set1.intersection(set2)

    na = len(set1)
    nb = len(set2)

    nd = na+nb
    t = len(inter_sec)

    result = 0 
    for i in range(0,t):
        result += cal_successful_draw_distribution(i,na,nb,nd)
    
    return result

def cal_successful_draw_distribution(s, na, nb, nd):
    result = nCr(na,s) * nCr(nd-na, nb-s) / nCr(nd,nb)
    return result
