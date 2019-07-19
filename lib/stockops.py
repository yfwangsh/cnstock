from pymongo import MongoClient
import tushare as ts
import pandas as pd
import numpy
import datetime
import unittest
from .datastore import *

class tradeDate(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(tradeDate, self).__init__(mgclient)
        self.collection='tradedata'
        self.keydictarray=['exchange','cal_date']
    def isTrade(self, date ,exchange='SSE'):
        try:
            ret = -1
            if exchange!='SZSE' or exchange!='SSE':
                exchange='SSE'
            query = {'exchange': exchange,'cal_date':date}
            myset = self.mongoClient[self.collection]
            rds = myset.find(query).sort([("cal_date",-1)])
            for rec in rds:
                ret = rec['is_open']
                break
            return ret
        except AssertionError as err:
            print(str(err))     
    def getlasttrade(self,date,exchange='SSE'):
        try:
            ret = None
            if exchange!='SZSE' or exchange!='SSE':
                exchange='SSE'
            query = {'exchange': exchange,'cal_date':date}
            myset = self.mongoClient[self.collection]
            rds = myset.find(query).sort([("cal_date",-1)])
            for rec in rds:
                ret = rec['pretrade_date']
                break
            return ret
        except AssertionError as err:
            print(str(err))             
    def getStoreTMRange(self, code):
        try:
            if code !='SZSE':
                code='SSE'
            query = {'exchange': code}
            return stockInfoStore.getcolrange(self.mongoClient, self.collection, query, 'cal_date')
        except AssertionError as err:
            print(str(err))       
    def getprocessData(self, code, stdate, eddate,src):
        return self.pro.trade_cal(exchange=code,start_date=stdate, end_date=eddate,fields='pretrade_date,exchange,cal_date,is_open')

    def initalldata(self, date=''):
        stdate = '20140101'
        if date =='':
            eddate = datetime.datetime.now().strftime('%Y%m%d')
        else:
            if stdate > date:
                eddate = stdate
                stdate = date
            else:
                eddate = date
        self.storeStockInfo(code='SSE', stdate=stdate,eddate=eddate,updateflag=True)
        self.storeStockInfo(code='SZSE', stdate=stdate,eddate=eddate,updateflag=True)
        
class stockopStore(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(stockopStore, self).__init__(mgclient)
        self.collection='tradeoper'
        self.tkstore = tickdataStore(self.mongoClient)
        self.dystore = dailydataStore(self.mongoClient)
        self.mfstore = moneyFlowdataStore(self.mongoClient)
        self.trdstore = tradeDate(self.mongoClient)
    def generateopdata(self, code, trade_date):
        knowopdic = {}
        if self.trdstore.isTrade(trade_date) != 1:
            return None
        df = self.tkstore.loadData(code, trade_date, trade_date)
        if df.empty:
            self.tkstore.storeProcess(code,trade_date, trade_date,'tt',False)
            df = self.tkstore.loadData(code, trade_date, trade_date)
            if df.empty:
                return None
        df1 = self.dystore.loadData(code, trade_date, trade_date)
        if df1.empty:
            return None
        '''
        交易中实际的最高值
        '''
        achigh= df.loc[df['price'].idxmax()].at['price'] 
        aclow =  df.loc[df['price'].idxmin()].at['price']
        high = df1.get("high").get_values()[0]
        low = df1.get("low").get_values()[0]
        '''
        开盘价和收盘价
        '''
        closep = df1.get("close").get_values()[0]
        openp = df1.get("open").get_values()[0]
        
        '''
        振幅，开盘价为基数
        '''
        todaymean = (high+low)/2
        '''
        收盘的时候，和最高值相比，越高卖出
        收盘时和最低值相比，越高买进
        '''
   
        '''
        波峰波谷指数
        
        '''
        vhigh = achigh * (1 - 0.002)
        vlow = aclow * (1 + 0.002)
        df2 = df.query('price>=@vhigh').assign(type='H')
        df3 = df.query('price<=@vlow').assign(type='L')
        df4 = df2.append(df3).sort_values(by=['time'])
        
        knowopdic['highstart'] = df2['time'].head(1).get_values()[0]
        knowopdic['highend'] = df2['time'].tail(1).get_values()[0]
        knowopdic['lowstart'] = df3['time'].head(1).get_values()[0]
        knowopdic['lowend'] = df3['time'].tail(1).get_values()[0]
        tmhighend = knowopdic['highend'] 
        tmlowstart = knowopdic['lowstart']
        df5 = df.query('time>=@tmhighend').sort_values(by=['price'])
        df6 = df.query('time<=@tmlowstart').sort_values(by=['price'])
        knowopdic['highendtolow'] = df5['price'].head(1).get_values()[0]
        knowopdic['lowstartbeforehigh'] = df6['price'].tail(1).get_values()[0]
        trendstr = stockInfoStore.removeDups(df4['type'].get_values())
        ndtradeinfo = self.dystore.getrecInfo(code, trade_date, 1)
        if ndtradeinfo is None:
            return None
        nxhigh = ndtradeinfo.get("high").get_values()[0]
        decval = ''
        knowopdic['ts_code']=stockInfoStore.canoncode(code)
        knowopdic['trade_date'] = trade_date
        knowopdic['open'] = openp
        knowopdic['close'] = closep
        knowopdic['high'] = high
        knowopdic['achigh'] = achigh
        knowopdic['low'] = low
        knowopdic['aclow'] = aclow
        knowopdic['nxhigh'] = nxhigh
        knowopdic['ampfull'] =  (achigh-low)/openp 
        knowopdic['ampprelow'] =  (achigh-openp)/openp 
        knowopdic['bsvalue'] = (nxhigh - low)/closep
        knowopdic['buyprob'] =  (nxhigh-todaymean)/closep
        knowopdic['sellprob'] =  (nxhigh-low)/closep 
        knowopdic['nopvalue'] = (nxhigh - high)/closep
        knowopdic['trend'] = trendstr
        
        '''
        振幅大于3%且时低->高的趋势，可以做波段
        '''
        '''
        明日最高比今日最高高1.5个点之上，买入是安全的
        '''
        if knowopdic['buyprob'] > 0.03 and  knowopdic['nopvalue'] >= 0.015:
            decval = '+'

        if knowopdic['sellprob'] < 0.015 and  knowopdic['nopvalue'] < 0.005:
            decval = '-'    

        if knowopdic['ampfull'] > 0.03 and 'LH' in trendstr:
            decval+='bswave'
        if knowopdic['ampfull'] > 0.03 and 'HL' in trendstr:
            if 'bswave' in decval:
                decval = decval.replace('bs','')
            else:
                if knowopdic['bsvalue'] > 0.03:
                    decval+='sbwave'
        if decval=='':
            decval='nop'
        knowopdic['decision'] = decval
        return knowopdic
    def getprocessData(self, code, stdate, eddate,src):
        datestart=datetime.datetime.strptime(stdate,'%Y%m%d')
        dateend=datetime.datetime.strptime(eddate,'%Y%m%d')
        arraydict = []
        while datestart<=dateend:
            tkdtstring=datestart.strftime('%Y%m%d')
            datestart+=datetime.timedelta(days=1)
            mydict = self.generateopdata(code, tkdtstring)
            if mydict is None :
                continue
            arraydict.append(mydict)
        return pd.DataFrame.from_records(arraydict)
    def storeDecision(self,code):
        curst, curend = self.getStoreTMRange(code)
        if curst is None:
            print('please manual initial decision store before call this')
            return False

        stdate = curend        
        eddate = datetime.datetime.now().strftime('%Y%m%d')
        self.storeStockInfo(code ,stdate,eddate,updateflag=True)

class storeoperator:
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        self.mongoClient = mgclient
        self.indiset = 'indicatorset'
        self.dystore = dailydataStore(self.mongoClient)
        self.mfstore = moneyFlowdataStore(self.mongoClient)
        self.tkstore = tickdataStore(self.mongoClient)
        self.dybstore = dybasicStore(self.mongoClient)
        self.sopstore = stockopStore(self.mongoClient)
        self.bgstock = bgstockInfo(self.mongoClient)
    def filldailydb(self):
        trydate = datetime.datetime.now().strftime('%Y%m%d')
        self.mfstore.storeAllStockInfo('20190630',trydate)
        self.dystore.storeAllStockInfo('20190630',trydate)
    def calrate(self, code):
        prvdt = None
        curdt = None
        prvmavar = None
        curmavar = None

        entrydf =  self.dystore.loadEntry(code)
        if entrydf.empty:
            return False
        dtlist = entrydf['trade_date'].get_values()
        count = 1
        size = len(dtlist)
        lowcount = 0
        if size < 10:
            return False
        for dt in dtlist:
            ma5 = self.dystore.getMa(code, dt)
            ma10 = self.dystore.getMa(code,dt,10)
            var = (ma5-ma10)/ma10
            if count == 9:
                prvdt = dt
                prvmavar = var
            if count == 10:
                curdt = dt
                curmavar = var
            if var < 0:
                lowcount = lowcount + 1
            count = count + 1
        if curdt <'20190701':
            return False
        cquery='trade_date==@curdt'
        pquery='trade_date==@prvdt'

        cchg1 = entrydf.query(cquery).get("pct_chg").get_values()[0]  
        pchg1 = entrydf.query(pquery).get("pct_chg").get_values()[0]  

        if curmavar > 0 and curmavar < 0.008 and prvmavar > -0.004 \
            and cchg1 <4 and pchg1<4 and (pchg1+cchg1)>0 and (lowcount/size) <=0.4 and  (lowcount/size) >0.2:
            print('[' + code + ']' )

    def generateAll(self, code):
        dyentry = self.dystore.loadEntry(code, line=120)
        resdyEntry=None
        resmfEntry=None
        if dyentry.empty:
            return dyentry
        query1='pct_chg>=5'
        bchg = dyentry.query(query1)  
        if bchg.empty:
            return bchg
        dtlist = bchg['trade_date'].get_values()
        prvdate = None
        for dt in dtlist:
            prvfound = False
            ent = self.dystore.loadEntry(code, date=dt, line = 5)
            mfent = self.mfstore.loadEntry(code, date=dt, line = 5)
            dtl = ent['trade_date'].get_values()
            if not prvdate is None:
                for idt in dtl:
                    if idt == prvdate:
                        prvfound = True
                        break
            if prvfound:
                resdyEntry = resdyEntry.append(ent.query('trade_date>@prvdate'))
                resmfEntry = resmfEntry.append(mfent.query('trade_date>@prvdate'))
            else:
                if resdyEntry is None:
                    resdyEntry = ent
                    resmfEntry = mfent
                else:
                    resdyEntry = resdyEntry.append(ent)
                    resmfEntry = resmfEntry.append(mfent)
            prvdate = dt
        resmfEntry = resmfEntry.assign(blgpct=(resmfEntry['buy_elg_vol'] + resmfEntry['buy_lg_vol'])/(resmfEntry['buy_elg_vol'] + resmfEntry['buy_lg_vol']+resmfEntry['buy_md_vol']+resmfEntry['buy_sm_vol']))
        resmfEntry = resmfEntry.assign(slgpct=(resmfEntry['sell_elg_vol'] + resmfEntry['sell_lg_vol'])/(resmfEntry['sell_elg_vol'] + resmfEntry['sell_lg_vol']+resmfEntry['sell_md_vol']+resmfEntry['sell_sm_vol']))

        newpd = pd.merge(resdyEntry.filter(items=['ts_code', 'trade_date','open','close','high','pct_chg','amount','vol']),\
            resmfEntry.filter(items=['ts_code', 'trade_date', 'blgpct','slgpct','net_mf_vol']), \
                on=['ts_code','trade_date'])
        ffdt = newpd['trade_date'].get_values()
        madf = None
        for fdt in ffdt:
            ma5 = self.dystore.getMa(code, fdt)
            ma10 = self.dystore.getMa(code, fdt,10)
            dicitem = {'ts_code': stockInfoStore.canoncode(code), 'trade_date': fdt, 'ma5': ma5, 'ma10': ma10 }
            if madf is None:
                madf = pd.DataFrame.from_records(dicitem,index=[0])
            else:
                madf = madf.append(pd.DataFrame.from_records(dicitem,index=[0]))
        newpd = pd.merge(newpd, madf,  on=['ts_code','trade_date'])
        return newpd

    def findpattern(self):
        df = self.bgstock.loadallstock()
        mdf = None
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            tmpdf = self.generateAll(code)
            if tmpdf.empty:
                continue
            if mdf is None:
                mdf = tmpdf
            else:
                mdf = mdf.append(tmpdf)
            print("code:"+ code + " done")
        mdf.to_excel('allhigh.xlsx')
        print('done')
    def findsuit(self):
        df = self.bgstock.loadallstock()
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            self.calrate(code)    
class bgstockInfo:
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        self.mongoClient = mgclient
        self.pro = Singleton().getPro()
        self.collection ='bstockset'
    def loadallstock(self):
        query={}
        return stockInfoStore.smgquery(self.mongoClient, self.collection, query)
    def savebgstockInfo(self):
        try:
            #arraydict = []
            df = self.pro.stock_basic()
            if df.empty:
                print("process store basic info empty")
                return False
            bstockset = self.mongoClient[self.collection]
            for i in range(0, df.index.size):
                bstockdic = df.iloc[i].to_dict()
                for k in bstockdic.keys():
                    if type(bstockdic[k]) == numpy.int64:
                        bstockdic[k] = int(bstockdic[k])
                    if type(bstockdic[k]) == numpy.float64:
                        bstockdic[k] = float(bstockdic[k])
                #arraydict.append(bstockdic)
                keydic={}
                keydic['ts_code'] = bstockdic['ts_code']
                bstockset.update_one(keydic,{'$set':{'upflag':1},'$setOnInsert': bstockdic}, upsert=True)
            return True
        except AssertionError as err:
            print(str(err)) 

if __name__ == "__main__":
    stoper = storeoperator()
    stoper.findsuit()