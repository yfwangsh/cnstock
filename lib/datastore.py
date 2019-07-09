from pymongo import MongoClient
import tushare as ts
import pandas as pd
import numpy
import datetime
import unittest
import threading

class Singleton(object):
    _instance_lock = threading.Lock()

    def __init__(self):
        ts.set_token("23b817c8b6e2b772f37ad6f5628ad348a0aefed07ed9b07ecc75976d")
        self.pro = ts.pro_api()   

    def getPro(self):
        return self.pro
    def __new__(cls, *args, **kwargs):
        if not hasattr(Singleton, "_instance"):
            with Singleton._instance_lock:
                if not hasattr(Singleton, "_instance"):
                    Singleton._instance = object.__new__(cls)  
        return Singleton._instance

'''
pattern:
Has ts_code, trade_date
'''
class stockInfoStore:
    def __init__(self,mgclient):
        self.mongoClient = mgclient
        #ts.set_token("23b817c8b6e2b772f37ad6f5628ad348a0aefed07ed9b07ecc75976d")
        #self.pro = ts.pro_api()    
        self.pro = Singleton().getPro()
        self.collection =''
        self.keydictarray=['ts_code','trade_date']
    @staticmethod
    def removeDups(stringArray):
        result = stringArray[0]
        for i in range(1,len(stringArray)):
            if stringArray[i] != stringArray[i-1]:
                result += stringArray[i]
        return result
    @staticmethod
    def canoncode(code):
        proquerycode = code
        if code.startswith('6'):
            proquerycode= proquerycode + ".SH"
        elif code.startswith('0') or code.startswith('3'):
            proquerycode= proquerycode + ".SZ"
        else:
            raise Exception('unknow stock code')
        return proquerycode
    @staticmethod
    def getshiftday(day,offset,fmt='%Y%m%d'):
        '''
        '%Y%m%d'
        '''
        day=datetime.datetime.strptime(day,fmt)
        day+=datetime.timedelta(days=offset)
        return day.strftime(fmt)
    
    @staticmethod
    def getcolrange(mgclient, collection, query, rgparam='trade_date'):
        try:
            myset = mgclient[collection]
            rds = myset.find(query).sort([(rgparam,-1)])
            svend = None
            svst = None
            for rec in rds:
                svend = rec[rgparam]
                break
            rds = myset.find(query).sort([(rgparam,1)])
            for rec in rds:
                svst = rec[rgparam]
                break
            return svst,svend
        except AssertionError as err:
            print(str(err))      
    def getStoreTMRange(self, code):
        query = {}
        if code != '':
            query = {"ts_code": stockInfoStore.canoncode(code)}
        return stockInfoStore.getcolrange(self.mongoClient, self.collection,query)

    def getrecInfo(self, code, trade_date, offset):
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$gt":trade_date}  }
        if offset == 0:
            query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": trade_date }
        elif offset < 0:
            query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$lt":trade_date}  }
                        
        df = self.mgquery(query)
        if df.empty:
            print("info empty，date=%s, offset=%d"%(trade_date,offset))
            return None
        if offset > 0:
            return df.sort_values(by=['trade_date']).head(offset).tail(1)
        elif offset == 0:
            return df
        else:
            return df.sort_values(by=['trade_date']).tail(offset).head(1)   
    def fetchBatchData(self, trade_date, src):
        raise NotImplementedError

    def batchStoreProcess(self, stdate, eddate, src,updateflag):
        try:
            datestart=datetime.datetime.strptime(stdate,'%Y%m%d')
            dateend=datetime.datetime.strptime(eddate,'%Y%m%d')
            storeset = self.mongoClient[self.collection]
            while datestart<=dateend:
                tkdtstring=datestart.strftime('%Y%m%d')
                df = self.fetchBatchData(trade_date=tkdtstring, src=src)
                datestart+=datetime.timedelta(days=1)
                if df is None or df.empty:
                    continue
                arraydict = []                
                for rr in df.itertuples(index=False):            
                    mydict = rr._asdict()
                    for k in mydict.keys():
                        if type(mydict[k]) == numpy.int64:
                            mydict[k] = int(mydict[k])
                        if type(mydict[k]) == numpy.float64:
                            mydict[k] = float(mydict[k])
                    arraydict.append(mydict)
                    if updateflag:
                        keydic={}
                        for dickey in self.keydictarray:
                            keydic[dickey] = mydict[dickey]
                        storeset.update_one(keydic,{'$set':{'upflag':1},'$setOnInsert': mydict}, upsert=True)
                if not updateflag:
                    storeset.insert_many(arraydict)
        except AssertionError as err:
            print(str(err))
    def storeProcess(self, code, stdate, eddate, src,updateflag):
        try:
            arraydict = []
            storeset = self.mongoClient[self.collection]
            df = self.getprocessData(code, stdate, eddate,src)
            if df is None or df.empty:
                return False
            for rr in df.itertuples(index=False):            
                mydict = rr._asdict()
                for k in mydict.keys():
                    if type(mydict[k]) == numpy.int64:
                        mydict[k] = int(mydict[k])
                    if type(mydict[k]) == numpy.float64:
                        mydict[k] = float(mydict[k])
                arraydict.append(mydict)
                if updateflag:
                    keydic={}
                    for dickey in self.keydictarray:
                        keydic[dickey] = mydict[dickey]
                    storeset.update_one(keydic,{'$set':{'upflag':1},'$setOnInsert': mydict}, upsert=True)
            if not updateflag:
                storeset.insert_many(arraydict)
            return True
        except AssertionError as err:
            print(str(err))          
    def storeStockInfo(self, code, stdate,eddate,src='tt',updateflag=False):
        try:
            assert(stdate <= eddate)
            curst, curend = self.getStoreTMRange(code)
            ret = False
            if curst is None:
                self.storeProcess(code, stdate,eddate, src,updateflag)
                return True
            if curst > stdate and curst <= eddate:
                self.storeProcess(code, stdate,stockInfoStore.getshiftday(day=curst,offset=-1),src,updateflag)
                ret = True
            if curend < eddate and curend >= stdate:
                self.storeProcess(code,stockInfoStore.getshiftday(day=curend,offset=1),eddate,src,updateflag)
                ret = True
            return ret
        except AssertionError as err:
            print(str(err))
    
    def getprocessData(self, code, stdate, eddate,src):
        raise NotImplementedError

    @staticmethod
    def smgquery(mgclient, collection, query, sort=None):
        try:
            rowset = mgclient[collection]
            rds = None
            if sort is None:
                rds = rowset.find(query)
            else:
                rds = rowset.find(query).sort(sort)
            arraydict = []
            for rec in rds:
                arraydict.append(rec)
            return pd.DataFrame.from_records(arraydict)
        except AssertionError as err:
            print(str(err))  
    def mgquery(self, query):
        return stockInfoStore.smgquery(self.mongoClient,self.collection, query)        
    def loadData(self, code, stdate,eddate):
        '''
        accepted ate format: '%Y%m%d'
        '''
        try:
            query = {"ts_code": stockInfoStore.canoncode(code) , "trade_date": {"$gte":stdate,"$lte": eddate}  }
            if stdate == eddate:
                query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": eddate  }
            return self.mgquery(query)
        except AssertionError as err:
            print(str(err))                                   
     
class dailydataStore(stockInfoStore):
    def __init__(self,mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(dailydataStore, self).__init__(mgclient)
        self.collection='dailyset'
    def getprocessData(self, code, stdate, eddate,src):
        return self.pro.query('daily', ts_code=stockInfoStore.canoncode(code), start_date=stdate, end_date=eddate)
    def fetchBatchData(self, trade_date, src):
         return self.pro.daily(trade_date=trade_date)

class moneyFlowdataStore(stockInfoStore):
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(moneyFlowdataStore, self).__init__(mgclient)
        self.collection='moneyflow'
    def getprocessData(self, code, stdate, eddate,src):
        return self.pro.moneyflow(ts_code=stockInfoStore.canoncode(code),start_date=stdate,end_date=eddate)
    def fetchBatchData(self, trade_date, src):
         return self.pro.moneyflow(trade_date=trade_date)

class tradeDate(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(tradeDate, self).__init__(mgclient)
        self.collection='tradedata'
        self.keydictarray=['exchange','cal_date']
  
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
        
class tickdataStore(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(tickdataStore, self).__init__(mgclient)
        self.collection='tickdata'
    def getprocessData(self, code, stdate, eddate,src):
        try:
            datestart=datetime.datetime.strptime(stdate,'%Y%m%d')
            dateend=datetime.datetime.strptime(eddate,'%Y%m%d')
            tmpdf = None
            while datestart<=dateend:
                tkdtstring=datestart.strftime('%Y-%m-%d')
                df = ts.get_tick_data(code=code,date=tkdtstring, src=src)
                datestart+=datetime.timedelta(days=1)
                if df is None or df.empty:
                    continue   
                if tmpdf is None:
                    tmpdf = df
                else:
                    tmpdf = tmpdf.append(df,ignore_index=True ) 
            return tmpdf
        except AssertionError as err:
            print(str(err)) 
    def storeProcess(self, code, stdate, eddate,src,updateflag):
        try:
            datestart=datetime.datetime.strptime(stdate,'%Y%m%d')
            dateend=datetime.datetime.strptime(eddate,'%Y%m%d')
            ret = False
            while datestart<=dateend:
                tkdtstring=datestart.strftime('%Y-%m-%d')
                df = ts.get_tick_data(code=code,date=tkdtstring, src=src)
                datestart+=datetime.timedelta(days=1)
                if df is None or df.empty:
                    continue
                arraydict = []                
                for i in range(0, df.index.size):
                    tkdic = df.iloc[i].to_dict()
                    for k in tkdic.keys():
                        if type(tkdic[k]) == numpy.int64:
                            tkdic[k] = int(tkdic[k])
                        if type(tkdic[k]) == numpy.float64:
                            tkdic[k] = float(tkdic[k])
                    tkdic['ts_code'] =stockInfoStore.canoncode(code)
                    tkdic['trade_date'] =tkdtstring.replace('-','')
                    arraydict.append(tkdic)
                tkset = self.mongoClient[self.collection]
                tkset.insert_many(arraydict)
                ret = True
            return ret
        except AssertionError as err:
            print(str(err))
    def test(self, stdt):
        mydf = self.pro.daily(trade_date=stdt)
        higtop = 10.5
        lowtop = 9.5
        df2 = mydf.query('pct_chg>=@lowtop and pct_chg<=@higtop')
        for rr in df2.itertuples(index=False):            
            print(rr._asdict())

class dybasicStore(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(dybasicStore, self).__init__(mgclient)
        self.collection='dailybasic'
        
    def getprocessData(self, code, stdate, eddate,src):
       return self.pro.daily_basic(ts_code=stockInfoStore.canoncode(code), start_date=stdate,end_date=eddate, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')
class stockopStore(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(stockopStore, self).__init__(mgclient)
        self.collection='tradeoper'
        self.tkstore = tickdataStore(self.mongoClient)
        self.dystore = dailydataStore(self.mongoClient)
        self.mfstore = moneyFlowdataStore(self.mongoClient)
    def generateopdata(self, code, trade_date):
        knowopdic = {}
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
        while datestart<=dateend:
            tkdtstring=datestart.strftime('%Y%m%d')
            arraydict = []
            datestart+=datetime.timedelta(days=1)
            mydict = self.generateopdata(code, tkdtstring)
            if mydict is None :
                continue
            arraydict.append(mydict)
        return pd.DataFrame.from_records(arraydict)
class storeoperator:
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        self.mongoClient = mgclient
        self.dystore = dailydataStore(self.mongoClient)
        self.mfstore = moneyFlowdataStore(self.mongoClient)
        self.tkstore = tickdataStore(self.mongoClient)
        self.dybstore = dybasicStore(self.mongoClient)
        self.sopstore = stockopStore(self.mongoClient)
    
if __name__ == "__main__":
    print("ol")
    tkstore = tickdataStore()
    print(tkstore.loadData('600639','20190627','20190627'))
    trd = tradeDate()
    #trd.initalldata()
    print(trd.getlasttrade('20190601'))