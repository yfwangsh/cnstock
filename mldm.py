from pymongo import MongoClient
import pandas as pd
import numpy
import datetime
import unittest
import os
import copy
import json
import time
import random
from sklearn.svm import SVC
import sys
sys.path.append(os.getcwd() + '/lib')

from lib.datastore import dailydataStore
from lib.datastore import stockInfoStore
from lib.datastore import moneyFlowdataStore
from lib.datastore import tickdataStore
from lib.stockops import tradeDate
from lib.stockops import storeoperator
from lib.stockops import pollst

class dtgenerator:
    def __init__(self):
        self.operator = storeoperator()
        self.divcache = {}
        self.debug = True
    def gentrdtmtbycode(self, code, date):
        dystore = self.operator.dystore
        resultdic = dict()
        #alld = dystore.LoadDataInfobyStock(code)
        rec = dystore.getrecInfo(code, date,0)
        if rec is None or rec.empty:
            return None     
        cprice = rec['close'].get_values()[0]
        cvol = rec['vol'].get_values()[0]
        clow = rec['low'].get_values()[0]
        chigh = rec['high'].get_values()[0]
        pct = rec['pct_chg'].get_values()[0]
        ma5 = dystore.getMa(code, date, 5)
        ma10 = dystore.getMa(code, date, 10)
        ma15 = dystore.getMa(code, date, 15)
        ma30 = dystore.getMa(code, date, 30)
        if ma5 is None or ma10 is None or ma15 is None or ma30 is None:
            return None
        if not self.chkdiv(code, date, cprice):
            return None
        pmaxclose = dystore.getppbound(code, date)
        pmaxhigh = dystore.getppbound(code, date, type='10')
        pminlow = dystore.getppbound(code, date, type='31')
        fmaxhigh = dystore.getppbound(code, date, direct=1, num = 5, type='10')
        if pmaxclose is None or pmaxhigh is None or pminlow is None or fmaxhigh is None:
            return None
        prvdt = dystore.getrecInfo(code, date, -1)['trade_date'].get_values()[0]
        pavgvol10 = dystore.getAvg(code, 'vol', prvdt, num=10)

        if self.debug:
            resultdic['rma5'] =ma5
            resultdic['rma10'] =ma10
            resultdic['rma15'] =ma15
            resultdic['rma30'] =ma30
            resultdic['rcprice'] =cprice
            resultdic['rcvol'] =cvol
            resultdic['rmxclose'] =pmaxclose
            resultdic['rmxhigh'] =pmaxhigh
            resultdic['rfmxhigh'] = fmaxhigh
            resultdic['ravgvol'] = pavgvol10
            resultdic['rminlow'] = pminlow
            resultdic['rchigh'] = chigh
            resultdic['rclow'] = clow
            

        resultdic['vma5'] =pollst.calpct(ma5, cprice)
        resultdic['vma10'] =pollst.calpct(ma10, cprice)
        resultdic['vma15'] =pollst.calpct(ma15, cprice)
        resultdic['vma30'] =pollst.calpct(ma30, cprice)
        resultdic['vmxclose'] =pollst.calpct(pmaxclose, cprice)
        resultdic['vmxhigh'] =pollst.calpct(pmaxhigh, cprice)
        resultdic['vminlow'] =pollst.calpct(pminlow, cprice)
        if self.debug:
            resultdic['vfmxhigh'] = pollst.calpct(fmaxhigh, cprice) 
        if pollst.calpct(fmaxhigh, cprice) >= 5:
            resultdic['vopt'] = 1
        else:
            resultdic['vopt'] = 0
        resultdic['vavgvol'] = pollst.calpct(pavgvol10, cvol)
        resultdic['vchigh'] = pollst.calpct(chigh, cprice)
        resultdic['vclow'] = pollst.calpct(clow, cprice)
        resultdic['vpct'] = pct

        return resultdic

    def chkdiv(self, code, date, price):
        divtup = self.getdiv(code, date)
        if divtup is None:
            return True
        exdate = divtup[0]
        cashdiv = divtup[1]
        stkdiv = divtup[2]
        prvchk = self.operator.dystore.getrecInfo(code, date, -30)
        fwdchk = self.operator.dystore.getrecInfo(code, date, 5)
        if prvchk is None or fwdchk is None:
            return False
        if exdate <= fwdchk['trade_date'].get_values()[0] and exdate >= prvchk['trade_date'].get_values()[0]:
            if stkdiv >= 0.1 or cashdiv/price >= 0.01:
                return False
        return True

    def getdiv(self, code, date):
        st = date[:4] + '0101'
        et = date[:4] + '1231'
        key = code + ":" + date[:4]
        rettup = None
        if self.divcache.__contains__(key):
            df = self.divcache[key] 
        else:
            alldiv = self.operator.dystore.pro.dividend(ts_code = self.operator.dystore.canoncode(code), fields='ts_code,stk_div,ex_date, cash_div_tax')
            df = alldiv.query('ex_date>=@st and ex_date<=@et')
            self.divcache[key] = df
        for rr in df.itertuples(index=False):
            mydict = rr._asdict()
            exdate = mydict['ex_date']
            cashdiv = mydict['cash_div_tax']
            stkdiv = mydict['stk_div']
            if rettup is None:
                rettup = (exdate, cashdiv, stkdiv)
            else:
                prvdt = rettup[0]
                if exdate > prvdt:
                    if int(date) >  (int(exdate) + int(prvdt))/2:
                        rettup = (exdate, cashdiv, stkdiv)
                else:
                    if int(date) <  (int(exdate) + int(prvdt))/2:
                        rettup = (exdate, cashdiv, stkdiv)
        return rettup 
    def gentrainforcode(self, code):
        ndf = self.operator.dystore.LoadDataInfobyStock(code)
        retlist = []
        for rr in ndf.itertuples(index=False):
            mydict = rr._asdict()
            trdate = mydict['trade_date']
            rec = self.gentrdtmtbycode(code, trdate)
            if rec is not None:
                retlist.append(rec)
        return retlist
        

    def gentrain(self):
        df = self.operator.bgstock.loadallstock()
        retlist = []
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            retlist = retlist + self.gentrainforcode(code)
            print("code:"+ code + " done")
            if random.randint(1,100) <= 20:
                break
        mdf = pd.DataFrame.from_records(retlist)
        mdf.to_excel('tr.xlsx')
        print('done')


if __name__ == "__main__":
    dtg = dtgenerator()
    print(dtg.operator.dystore.getrecInfo('600824', '20180114',-30))
    print(dtg.operator.dystore.getppbound('600824', '20180115', type='31'))
    print(dtg.getdiv('000003', '20181011'))
    dtg.debug=False
    dtg.gentrain()