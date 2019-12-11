from pymongo import MongoClient
import tushare as ts
import pandas as pd
import numpy
import datetime
import time
import unittest
import threading
import copy
from bs4 import BeautifulSoup
import requests
from configparser import ConfigParser

class Singleton(object):
    _instance_lock = threading.Lock()
    def getConf(self, section, key):
        return self.config.get(section, key)
    def initConf(self, file):
        cp = ConfigParser()
        cp.read(file)
        return cp
    def __init__(self):
        self.pro = ts.pro_api()   
        self.config = self.initConf('conf/mystock.conf')
        token = self.getConf('tushare', 'token')
        ts.set_token(token)
    def getPro(self):
        return self.pro
    def __new__(cls, *args, **kwargs):
        if not hasattr(Singleton, "_instance"):
            with Singleton._instance_lock:
                if not hasattr(Singleton, "_instance"):
                    Singleton._instance = object.__new__(cls)  
        return Singleton._instance
class dbbase:
    def __init__(self,mgclient):
        self.mongoClient = mgclient
        self.collection =''
        self.keydictarray=['ts_code','trade_date']

class Analystpool(dbbase):
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(Analystpool, self).__init__(mgclient)
        self.collection='analystpool'
        self.keydictarray=['code','trade_date']
    def setMonitoring(self, code, trade_date, mprice, flag=True, rule='strategy1', dtkey=True):
        storeset = self.mongoClient[self.collection]
        monitor = 0
        if flag:
            monitor = 1
        updic = {}
        updic['Monitorflag'] = monitor
        if mprice is not None:
            updic['Pricetarget'] = mprice
        updic['lastupdated'] = datetime.datetime.now().strftime('%Y%m%d')
        keydic={}
        keydic[self.keydictarray[0]] = code
        if dtkey:
            keydic[self.keydictarray[1]] = trade_date
        else:
            updic[self.keydictarray[1]] = trade_date
        keydic['match_rule'] = rule
        storeset.update_one(keydic,{'$set': updic}, upsert=True)
    def removeRec(self, code, rule='strategy1', trade_date=None):
        storeset = self.mongoClient[self.collection]
        query = {'code': code, 'match_rule': rule}
        if trade_date is not None:
            query = {'code': code, 'match_rule': rule, 'trade_date': trade_date}
        x = storeset.delete_many(query)
        return x.deleted_count
    def setManualFlag(self, code, trade_date, mprice, fmflag=False, rule='strategy1', dtkey=True):
        storeset = self.mongoClient[self.collection]
        updic = {}
        if mprice is not None:
            updic['mpflag'] = 1
            updic['Pricetarget'] = mprice
        else:
            updic['mpflag'] = 0
        monitor = 0
        if fmflag:
            monitor = 1
        updic['fmflag'] = monitor
        updic['lastupdated'] = datetime.datetime.now().strftime('%Y%m%d')
        keydic={}
        keydic[self.keydictarray[0]] = code
        if dtkey:
            keydic[self.keydictarray[1]] = trade_date
        else:
            updic[self.keydictarray[1]]
        keydic['match_rule'] = rule
        storeset.update_one(keydic,{'$set': updic}, upsert=True)

    def saveRec(self,dicrec):
        storeset = self.mongoClient[self.collection]
        keydic={}
        cpdict = copy.deepcopy(dicrec)
        for dickey in self.keydictarray:
            keydic[dickey] = dicrec[dickey]
            del cpdict[dickey]
        keydic['match_rule'] = dicrec['match_rule']
        del cpdict['match_rule']
        storeset.update_one(keydic,{'$setOnInsert': {'upflag':1}, '$set':cpdict}, upsert=True)
    def loadData(self,trade_date, rule='strategy1', dtkey=True):        
        query = {self.keydictarray[1]: trade_date, 'match_rule': rule }
        if not dtkey:
            query =  {'match_rule': rule }
        df = stockInfoStore.smgquery(self.mongoClient,self.collection, query)
        return df
    def fetchDataEntryByCode(self, code, rule='strategy1' ):
        query = {self.keydictarray[0]: code, 'match_rule': rule }
        df = stockInfoStore.smgquery(self.mongoClient,self.collection, query)
        return df

    def fetchDataEntry(self,trade_date, end_date=None, rule='strategy1'):
        rdate = end_date
        if end_date is None:
            rdate = datetime.datetime.now().strftime('%Y%m%d')
        query = {self.keydictarray[1]: {"$gte":trade_date, "$lte":rdate} ,'match_rule': rule }
        df = stockInfoStore.smgquery(self.mongoClient,self.collection, query)
        return df
'''
pattern:
Has ts_code, trade_date
'''
class stockInfoStore:
    def __init__(self,mgclient):
        self.mongoClient = mgclient
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
            if not code.endswith('.SH'):
                proquerycode= proquerycode + ".SH"
        elif code.startswith('0') or code.startswith('3') :
            if not code.endswith('.SZ'):
                proquerycode= proquerycode + ".SZ"
        elif code=='SSE' or code=='SZSE':
            pass
        else:
            raise Exception('unknow stock code')
        return proquerycode
    @staticmethod
    def snfmcode(code):
        proquerycode = code
        if code.startswith('6'):
            proquerycode= 'sh' + proquerycode
        elif code.startswith('0') or code.startswith('3') :
            proquerycode= 'sz' + proquerycode
        elif code=='sh' or code=='sz':
            pass
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
    def getStoreTMRange(self, code=''):
        query = {}
        if code != '':
            query = {"ts_code": stockInfoStore.canoncode(code)}
        return stockInfoStore.getcolrange(self.mongoClient, self.collection,query)

    def getrecInfo(self, code, trade_date, offset):
        query = {self.keydictarray[0]: stockInfoStore.canoncode(code), self.keydictarray[1]: {"$gt":trade_date}  }
        if offset == 0:
            query = {self.keydictarray[0]: stockInfoStore.canoncode(code), self.keydictarray[1]: trade_date }
        elif offset < 0:
            query = {self.keydictarray[0]: stockInfoStore.canoncode(code), self.keydictarray[1]: {"$lt":trade_date}  }
                        
        df = self.mgquery(query)
        if df.empty or len(df.values) < abs(offset):
            #print("info empty，date=%s, offset=%d"%(trade_date,offset))
            return None
        if offset > 0:
            return df.sort_values(by=[self.keydictarray[1]]).head(offset).tail(1)
        elif offset == 0:
            return df
        else:
            return df.sort_values(by=[self.keydictarray[1]]).tail(-1*offset).head(1)   
    def fetchBatchData(self, trade_date, src):
        raise NotImplementedError
    def storeAllStockInfo(self, stdate,eddate,src='tt',updateflag=False):
        try:
            assert(stdate <= eddate)
            curst, curend = self.getStoreTMRange()
            print("start date  =%s, end date=%s in store"%(curst, curend))
            ret = False
            if curst is None:
                self.batchStoreProcess(stdate,eddate, src,updateflag)
                return True
            if curst > stdate and curst <= eddate:
                self.batchStoreProcess(stdate,stockInfoStore.getshiftday(day=curst,offset=-1),src,updateflag)
                ret = True
            if curend < eddate and curend >= stdate:
                self.batchStoreProcess(stockInfoStore.getshiftday(day=curend,offset=1),eddate,src,updateflag)
                ret = True
            return ret
        except AssertionError as err:
            print(str(err))
    def batchStoreProcess(self, stdate, eddate, src,updateflag=False):
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
    def LoadDataInfobyStock(self, code):
        curst, curend = self.getStoreTMRange(code)
        if curst is None:
            curst = datetime.datetime.now().strftime('%Y%m%d')
            curend = datetime.datetime.now().strftime('%Y%m%d')
        return self.loadData(code, curst, curend)
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
    def mgquery(self, query,sort=None):
        return stockInfoStore.smgquery(self.mongoClient,self.collection, query,sort)
    def loadEntry(self, code, date='',  line=10, prv=True):
        ndf = self.LoadDataInfobyStock(code)
        rtdate = date
        if ndf.empty:
            return ndf
        if date == '':
            rtdate = datetime.datetime.now().strftime('%Y%m%d')
        querystring = self.keydictarray[1] + '<=@rtdate'
        if not prv:
            querystring = self.keydictarray[1] + '>=@rtdate'
            vdf = ndf.query(querystring).sort_values(by=[self.keydictarray[1]]).head(line)
            return vdf
        #print(querystring)
        vdf = ndf.query(querystring).sort_values(by=[self.keydictarray[1]]).tail(line)
        return vdf


    def loadData(self, code, stdate,eddate):
        '''
        accepted ate format: '%Y%m%d'
        '''
        try:
            query = {self.keydictarray[0]: stockInfoStore.canoncode(code) , self.keydictarray[1]: {"$gte":stdate,"$lte": eddate}  }
            if stdate == eddate:
                query = {self.keydictarray[0]: stockInfoStore.canoncode(code), self.keydictarray[1]: eddate  }
            #print(query)
            return self.mgquery(query)
        except AssertionError as err:
            print(str(err))                                   
     
class dailydataStore(stockInfoStore):
    def __init__(self,mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(dailydataStore, self).__init__(mgclient)
        #self.collection='dailyset'
        self.collection = 'allstockset'
    def storeProcess(self, code, stdate, eddate, src,updateflag):
        print("dailystore only support batch store")
        pass
    def getprocessData(self, code, stdate, eddate,src):
        #return self.pro.query('daily', ts_code=stockInfoStore.canoncode(code), start_date=stdate, end_date=eddate)
        print("dailystore only support batch store")
        pass

    def fetchBatchData(self, trade_date, src):
        return self.pro.daily(trade_date=trade_date)
    
    def getMa(self,code, trade_date, num = 5):
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": trade_date  }
        df = self.mgquery(query)
        if df.empty:
            #print("Not a trade date!")
            return None
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$lte":trade_date}  }
        sort = [("trade_date",-1)]
        df = self.mgquery(query, sort)
        if df.empty:
            print("can not caculate ma")
        mylist = df['close'].head(num).get_values()
        return round(numpy.average(mylist),2)
    '''
    type: 
    00 - close high
    01 - close low
    10 - high high
    11 - high low
    20 - open high
    21 - open low
    30 - low high
    31 - low low
    40 - vol high
    41 - vol low
    '''
    def getppbound(self, code, trade_date, num=30, direct=0, type='00'):
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": trade_date  }
        df = self.mgquery(query)
        if df.empty:
            #print("Not a trade date!")
            return None
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$lt":trade_date}  }
        if not direct == 0:
            query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$gt":trade_date}  }
        sort = [("trade_date",-1)]
        df = self.mgquery(query, sort)
        if df.empty:
            print("can not caculate")
            return None
        if type[0] == '0':
            ctflg = 'close'
        elif type[0] == '1':
            ctflg = 'high' 
        elif type[0] == '2':
            ctflg = 'open' 
        elif type[0] == '3':
            ctflg = 'low' 
        elif type[0] == '4':
            ctflg = 'vol'
        mylist = df[ctflg].head(num).get_values()
        if not direct == 0:
            mylist = df[ctflg].tail(num).get_values()
        if type[1] == '0':
            return numpy.max(mylist)
        return numpy.min(mylist)
    
    def getAvg(self, code, field, trade_date, num=100):
        entrys = self.loadEntry(code, trade_date, num)
        if entrys.empty:
            return None
        return round(numpy.average(entrys[field]),2)

    def getStd(self, code, field, trade_date, num=100):
        entrys = self.loadEntry(code, trade_date, num)
        if entrys.empty:
            return None
        return round(numpy.std(entrys[field]),2)
    
    def pesMa(self, code, price, trade_date='', num=5):
        assert(num > 0)
        date = trade_date
        if trade_date=='':
            date = datetime.datetime.now().strftime('%Y%m%d')
        else:
            query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": date  }
            df = self.mgquery(query)
            if df.empty:
                #print("Not a trade date!")
                return None
        query = {"ts_code": stockInfoStore.canoncode(code), "trade_date": {"$lte":date}  }
        sort = [("trade_date",-1)]
        df = self.mgquery(query, sort)
        if df.empty:
            print("can not caculate ma")
        mylist = df['close'].head(num-1).get_values()
        return numpy.average(numpy.append(mylist, price))
class dybasicStore(stockInfoStore):
    def __init__(self,  mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(dybasicStore, self).__init__(mgclient)
        self.collection='dailybasic'
    def storeProcess(self, code, stdate, eddate, src,updateflag):
        print("dailybasic only support batch store")
        pass    
    def getprocessData(self, code, stdate, eddate,src):
        #return self.pro.daily_basic(ts_code=stockInfoStore.canoncode(code), start_date=stdate,end_date=eddate, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb')
        print("dailybasic only support batch store")
        pass
    def fetchBatchData(self, trade_date, src):
        return self.pro.daily_basic(trade_date=trade_date,fields='ts_code,trade_date,turnover_rate,turnover_rate_f, volume_ratio,pe,pb,total_share,float_share,free_share,total_mv,circ_mv')

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
    def clawtickdata(self, code , date):
        ccode = stockInfoStore.snfmcode(code)
        ipg = 1
        targetUrl = 'http://market.finance.sina.com.cn/transHis.php?symbol=' + ccode + '&date=' + date + '&page='
        my_headers = {
        'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12H143',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding' : 'gzip',
        'Accept-Language' : 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4'
        }
        fdn_list = []
        try:
            while True:
                fturl = targetUrl + str(ipg)
                sss = requests.Session()
                r = sss.get(fturl, headers = my_headers)  
                r.encoding = 'gb2312'
                soup = BeautifulSoup(r.text, 'lxml') 
                tables = soup.select('table')
                df_list = []
                for table in tables:
                    df_list.append(pd.concat(pd.read_html(table.prettify())))
                df = pd.concat(df_list)
                newdf = df.rename(index=str,columns={"成交时间":"time", "成交价":"price", "价格变动":"change", "成交量(手)": "volume", "成交额(元)":"amount","性质":"type"})
                if newdf.empty:
                    break
                fdn_list.append(newdf.replace('--', 0))
                ipg = ipg + 1
                time.sleep(0.1)
            fndf = pd.concat(fdn_list)
            fndf.reset_index(drop=True, inplace=True)
            return fndf
        except Exception as ex:
            print('exception:' + str(ex))
            return None
    
    def storefromClaw(self, code, date):
        try:
            datestart=datetime.datetime.strptime(date,'%Y%m%d')
            ret = False
            tkdtstring=datestart.strftime('%Y-%m-%d')
            df = self.clawtickdata(code, tkdtstring)
            if df is None:
                return ret
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
class moneyFlowdataStore(stockInfoStore):
    def __init__(self, mgclient= MongoClient('mongodb://localhost:27017/')['stock']):
        super(moneyFlowdataStore, self).__init__(mgclient)
        #self.collection='moneyflow'
        self.collection='allmfset'
    def storeProcess(self, code, stdate, eddate, src,updateflag):
        print("money flow only support batch store")
        pass
    def getprocessData(self, code, stdate, eddate,src):
        print("money flow only support batch store")
        pass
    def fetchBatchData(self, trade_date, src):
         return self.pro.moneyflow(trade_date=trade_date)