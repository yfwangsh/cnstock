from pymongo import MongoClient
import tushare as ts
import pandas as pd
import numpy
import datetime
import unittest
from .datastore import *

import requests
import json

class DingTalk_Base:
    def __init__(self):
        self.__headers = {'Content-Type': 'application/json;charset=utf-8'}
        self.url = ''
    def send_msg(self,text):
        json_text = {
            "msgtype": "text",
            "text": {
                "content": text
            },
            "at": {
                "atMobiles": [
                    ""
                ],
                "isAtAll": False
            }
        }
        return requests.post(self.url, json.dumps(json_text), headers=self.__headers).content
class DingTalk(DingTalk_Base):
    def __init__(self):
        super().__init__()
        # 填写机器人的url
        self.url = ''
    def setURL(self, url):
        self.url = url

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
    def isTradingNow(self):
        st, end = self.getStoreTMRange('SSE')
        today = datetime.datetime.now().strftime('%Y%m%d')
        if today > end:
            self.initalldata(today)
        if self.isTrade(today):
            nowtime = datetime.datetime.now()
            amtmstart =datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 9,30,0,0)
            amtmbreak = datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 11,30,0,0)
            pmtmstart = datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 13,00,0,0)
            pmtmend = datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 15,00,0,0)
            if (nowtime >  amtmstart and  nowtime < amtmbreak) or (nowtime >  pmtmstart and  nowtime < pmtmend):
                return True
        return False 

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
        self.paramterset = 'paramset'
        self.paramField = 'paramname'
        self.valueField = 'paramvalue'
        self.paramThreadRun = 'PriceMonitorRuning'
        self.debug = True

        self.dystore = dailydataStore(self.mongoClient)
        self.mfstore = moneyFlowdataStore(self.mongoClient)
        self.tkstore = tickdataStore(self.mongoClient)
        self.dybstore = dybasicStore(self.mongoClient)
        self.sopstore = stockopStore(self.mongoClient)
        self.bgstock = bgstockInfo(self.mongoClient)
        self.analyst = Analystpool(self.mongoClient)
        self.tradedate = tradeDate(self.mongoClient)
        self.dtalk = DingTalk()
    def filldailydb(self):
        trydate = datetime.datetime.now().strftime('%Y%m%d')
        self.mfstore.storeAllStockInfo('20190630',trydate)
        self.dystore.storeAllStockInfo('20190630',trydate)
        self.dybstore.storeAllStockInfo('20190630',trydate)
    
    @staticmethod
    def getRealQuote(code):
        df = ts.get_realtime_quotes(code) #Single stock symbol
        return df[['code','name','price','bid','ask','volume','amount','time']]
    def setParam(self, name, value):
        keydic = {self.paramField: name}
        mydict = {}
        mydict[self.paramField] = name
        mydict[self.valueField] = str(value)
        paramset = self.mongoClient[self.paramterset]
        paramset.update_one(keydic,{'$set':mydict,'$setOnInsert': {'upflag':1}}, upsert=True)
    def getParam(self, name):
        query =  {self.paramField: name}
        rowset = self.mongoClient[self.paramterset]
        rds = rowset.find(query)
        for rec in rds:
            if rec.__contains__(self.valueField):
                return rec[self.valueField]
        return None

    def setThreadRun(self, running=True):
        self.setParam(self.paramThreadRun, str(running) )
    
    def getThreadRunningFlag(self):
        value = self.getParam(self.paramThreadRun)
        if not value is None and value =='False':
            return False
        return True
    def updateMonitorSet(self, line=12):
        trade_date = datetime.datetime.now().strftime('%Y%m%d')
        lasttrade = trade_date
        if not self.tradedate.isTrade(trade_date):
            lasttrade = self.tradedate.getlasttrade(trade_date)
        count = 1
        while count < line:
            count = count + 1
            lasttrade = self.tradedate.getlasttrade(lasttrade)
        df = self.analyst.fetchDataEntry(lasttrade)
        val='False'
        wdf = df.query('result==@val')
        for rr in wdf.itertuples(index=False):
            mydict = rr._asdict()
            code = mydict['code']
            date = mydict['trade_date']
            lowmindic = self.lowPriceAfterDict(code, date)
            PriceTarget = self.getProposed(code, date)
            recentry = df.query('code==@code')
            if not date == max(recentry['trade_date']):
                self.analyst.setMonitoring(code, date, PriceTarget,False)
                continue
            if not lowmindic is None:
                Pricelow = lowmindic['low']
                if PriceTarget < Pricelow :
                    varpct = round((PriceTarget - Pricelow)/PriceTarget * 100,2)
                    if varpct > -10 and varpct < -0.3:
                        self.analyst.setMonitoring(code, date,PriceTarget)
                    else:
                        self.analyst.setMonitoring(code, date, PriceTarget, flag=False)
                else:
                    self.analyst.setMonitoring(code, date,PriceTarget, flag=False)
            else:
                self.analyst.setMonitoring(code, date,PriceTarget)
                
    def getMonitorSet(self, line=12):
        trade_date = datetime.datetime.now().strftime('%Y%m%d')
        lasttrade = trade_date
        if not self.tradedate.isTrade(trade_date):
            lasttrade = self.tradedate.getlasttrade(trade_date)
        count = 1
        while count < line:
            count = count + 1
            lasttrade = self.tradedate.getlasttrade(lasttrade)
        df = self.analyst.fetchDataEntry(lasttrade)
        wdf = df.query('Monitorflag==1')
        return wdf     
    def getMonitorDaySet(self, date):
        df = self.analyst.loadData(date)
        wdf = df.query('Monitorflag==1')
        return wdf

    def pdavailable(self, code, date, offset=0):
        dyoffset = offset + 2
        entry = self.dystore.getrecInfo(code, date, dyoffset)
        if entry is None:
            return False
        return True
    def checkpredict(self,code, date,thresh=2.5):
        entry = self.dystore.loadEntry(code, date, line=12, prv=False)
        entsize = len(entry['trade_date'].get_values())
        if entry is None or entry.empty:
            return False
        if entsize <=2:
            return False
        tpmax = numpy.max(entry.head(entsize).tail(entsize-2)['close'])
        pctmax = numpy.max(entry.head(entsize).tail(entsize-2)['pct_chg'])
        maxentry = entry.head(entsize).tail(entsize-2).query('close==@tpmax and trade_date>@date')
        maxptentry = entry.head(entsize).tail(entsize-2).query('pct_chg==@pctmax and trade_date>@date')
        if maxentry.empty or maxptentry.empty:
            return False
        tptmax = maxptentry['close'].get_values()[0]
        dt = maxentry['trade_date'].get_values()[0]
        pdt = maxptentry['trade_date'].get_values()[0]
        tmpent = entry.head(entsize).tail(entsize-1).query('trade_date<@dt and trade_date>@date')
        tmpptent = entry.head(entsize).tail(entsize-1).query('trade_date<@pdt and trade_date>@date')
        if tmpent.empty or tmpptent.empty:
            return False
        lowmin = numpy.min(tmpent['close'])
        lowptmin = numpy.min(tmpptent['close'])
        minentry = entry.head(entsize).tail(entsize-1).query('trade_date<@dt and close==@lowmin and trade_date>@date')
        minptentry = entry.head(entsize).tail(entsize-1).query('trade_date<@pdt and close==@lowptmin and trade_date>@date')
        if minentry.empty or minptentry.empty:
            return False
        lowdt = minentry['trade_date'].get_values()[0]
        lowpdt = minptentry['trade_date'].get_values()[0]

        rpct = round((tpmax - lowmin)/lowmin * 100, 2)
        rpcdt = round((tptmax-lowptmin)/lowptmin *100, 2)
        if rpct >= thresh or rpcdt >= thresh:
            return True
        return False
    def checkright(self,code, date,thresh=2.5):
        entry = self.dystore.getrecInfo(code, date, 2)
        if entry is None:
            return False
        if not entry.empty and entry['pct_chg'].get_values()[0] >= thresh:
            return True
        return False
    def checkp(self,code, date,thresh=2.5):
        entry = self.dystore.getrecInfo(code, date, 2)
        entry2 = self.dystore.getrecInfo(code, date, 3)
        if entry is None or entry2 is None:
            return False
        if (entry2['pct_chg'].get_values()[0] + entry['pct_chg'].get_values()[0]) > 0 \
            and entry2['pct_chg'].get_values()[0] >= thresh:
            return True
    def genresultfld(self, code, date):
        resdic={}
        resdic['ts_code'] = code
        resdic['trade_date'] = date
        entry = self.dystore.loadEntry(code, date, line=12, prv=False)
        if entry is None or entry.empty:
            return resdic
        curentry = entry.query('trade_date==@date')
        if curentry.empty:
            return resdic
        resdic['pct_chg'] = curentry['pct_chg'].get_values()[0]
        entsize = len(entry['trade_date'].get_values())
        if entsize <=2:
            return resdic
        tpmax = numpy.max(entry.head(entsize).tail(entsize-2)['close'])
        tphigh = numpy.max(entry.head(entsize).tail(entsize-2)['high'])
        pctmax = numpy.max(entry.head(entsize).tail(entsize-2)['pct_chg'])
        maxentry = entry.head(entsize).tail(entsize-2).query('close==@tpmax and trade_date>@date')
        maxptentry = entry.head(entsize).tail(entsize-2).query('pct_chg==@pctmax and trade_date>@date')
        highentry = entry.head(entsize).tail(entsize-2).query('high==@tphigh and trade_date>@date')
        if maxentry.empty or maxptentry.empty or highentry.empty:
            return resdic
        maxclosehigh = maxentry['high'].get_values()[0]
        tptmax = maxptentry['close'].get_values()[0]
        tpthigh = maxptentry['high'].get_values()[0]
        dt = maxentry['trade_date'].get_values()[0]
        pdt = maxptentry['trade_date'].get_values()[0]
        highdt = highentry['trade_date'].get_values()[0]
        tmpent = entry.head(entsize).tail(entsize-1).query('trade_date<@dt and trade_date>@date')
        tmpptent = entry.head(entsize).tail(entsize-1).query('trade_date<@pdt and trade_date>@date')

        if tmpent.empty or tmpptent.empty:
            return resdic
        closemin = numpy.min(tmpent['close'])
        closeptmin = numpy.min(tmpptent['close'])
        minentry = entry.head(entsize).tail(entsize-1).query('trade_date<@dt and close==@closemin and trade_date>@date')
        minptentry = entry.head(entsize).tail(entsize-1).query('trade_date<@pdt and close==@closeptmin and trade_date>@date')
        if minentry.empty or minptentry.empty:
            return resdic
        closelowdt = minentry['trade_date'].get_values()[0]
        closelowpdt = minptentry['trade_date'].get_values()[0]
        mincloselow =  minentry['low'].get_values()[0]
        mincloselowp = minptentry['low'].get_values()[0]
        resdic['minclosedate'] = closelowdt
        resdic['minclose'] =closemin
        resdic['mincloselow'] =mincloselow
        resdic['maxclosedate'] = dt
        resdic['maxclose'] = tpmax
        resdic['maxclosehigh'] =maxclosehigh
        
        resdic['minclosepdate'] =closelowpdt
        resdic['minptclose'] = closeptmin
        resdic['minptcloselow'] = mincloselowp
        resdic['maxcloseptdate'] = pdt
        resdic['maxclose'] = tptmax
        resdic['maxclosehigh'] = tpthigh
        
        resdic['maxhighdate'] = highdt
        resdic['maxhigh'] = tphigh
        rpct = round((tpmax - closemin)/closemin * 100, 2)
        rpcdt = round((tptmax-closeptmin)/closeptmin *100, 2)
        resdic['closerate'] = rpct
        resdic['pctchgrate'] = rpcdt
        return resdic
    def saveanadb(self, code, trade_date, matchRule, predresult):
        #df = self.dystore.getrecInfo(code, trade_date, offset=0)
        entrydic = {}
        entrydic['code'] = code
        entrydic['trade_date'] = trade_date
        entrydic['match_rule'] = matchRule
        entrydic['result']  = predresult
        entrydic['lastupdated'] = datetime.datetime.now().strftime('%Y%m%d')
        self.analyst.saveRec(entrydic)
    def getMfLowPrice(self, code, trade_date, refprice):
        entry = self.dystore.loadEntry(code, trade_date, line=12, prv=False)
        if entry is None or entry.empty:
            return None
        if len(entry.get_values()) <= 1:
            return None
        pentry = entry.tail(len(entry.get_values())-1)
        lowmin =  numpy.min(pentry['low'])
        lowminentry = pentry.query('low==@lowmin')
        topp = round(refprice * 1.015,2)
        lowp = round(refprice * 0.985,2)

        stentry = pentry.query('low<=@topp and low>=@lowp')
        if stentry.empty:
            retdic={}
            retdic['trade_date'] = lowminentry['trade_date'].get_values()[0]
            df = self.dystore.getrecInfo(code, retdic['trade_date'], offset=1)
            if df is None or df.empty:
                return None
            retdic['pricelow'] = lowmin
            retdic['pricelowchg'] = round((df['high'].get_values()[0] - lowmin)/lowmin*100,2)
            return retdic
        retdic= {}
        for rr in stentry.itertuples(index=False):     
            mydict = rr._asdict()
            date = mydict['trade_date']
            df = self.dystore.getrecInfo(code, date, offset=1)
            if df is None or df.empty:
                continue
            nhigh = df['high'].get_values()[0]
            clow = mydict['low']
            chgpt =  round((nhigh - clow) / clow * 100, 2)
            if not retdic.__contains__('pricelowchg') or chgpt > retdic['pricelowchg']:
                retdic['trade_date'] = date
                retdic['pricelow'] = clow
                retdic['pricelowchg'] =chgpt
        if not retdic.__contains__('pricelowchg'):
            return None
        return retdic
    def lowPriceAfterDict(self, code, trade_date):
        entry = self.dystore.loadEntry(code, trade_date, line=12, prv=False)
        if entry is None or entry.empty:
            return None
        if len(entry.get_values()) <= 1:
            return None
        pentry = entry.tail(len(entry.get_values())-1)
        lowmin = numpy.min(pentry['low'])        
        lowminentry = pentry.query('low==@lowmin')
        retdic = {}
        retdic['low'] = lowmin
        retdic['lowdate'] = lowminentry['trade_date'].get_values()[0]
        return retdic
    def lowPriceAfter(self, code, trade_date):
        entry = self.dystore.loadEntry(code, trade_date, line=12, prv=False)
        if entry is None or entry.empty:
            return None
        if len(entry.get_values()) <= 1:
            return None
        return numpy.min(entry.tail(len(entry.get_values())-1)['low'])
    def getMatchedSellPrice(self, code, trade_date):
        entry = self.dystore.loadEntry(code, trade_date, line=12, prv=False)
        if entry is None or entry.empty:
            return None
        if len(entry.get_values()) <= 1:
            return None
    
        return numpy.min(entry.tail(len(entry.get_values())-1)['low'])
    def getProposed(self, code, trade_date):
        retval = None
        df = self.dystore.getrecInfo(code, trade_date, offset=0)
        dfprev = self.dystore.getrecInfo(code, trade_date, offset=-1)
        dfpprev = self.dystore.getrecInfo(code, trade_date, offset=-2)
        if df is None or dfprev is None or dfpprev is None:
            return retval
        cchg = df['pct_chg'].get_values()[0]
        pchg = dfprev['pct_chg'].get_values()[0]
        
        if (cchg+pchg)>=15:
            retval = round(df['low'].get_values()[0] * (100-cchg)/100, 2)
            return retval

        if (cchg+pchg)<15 and (cchg+pchg)>=11:
            retval = round(df['close'].get_values()[0] * (100-(cchg+pchg)/2)/100, 2)
            return retval

        if cchg > 0 and pchg <= 4 and (cchg+pchg)<11:
            hgprice = max(dfprev['open'].get_values()[0],dfprev['close'].get_values()[0])  
            retval = round((dfprev['high'].get_values()[0]+hgprice)/2, 2) 
            return retval 
        
        if cchg > 0 and pchg > 4 and (cchg+pchg)<11:
            hgprice = max(dfpprev['open'].get_values()[0],dfpprev['close'].get_values()[0])  
            retval = round(hgprice, 2) 
            return retval 

    def calrate(self, code, date=''):
        prvmavar = None
        curmavar = None

        entrydf =  self.dystore.loadEntry(code, date, 15)
        dybdf = self.dybstore.loadEntry(code, date, 15)
        #mfentry = self.mfstore.loadEntry(code, date, 15)
        if entrydf.empty:
            return False
        dtlist = entrydf['trade_date'].get_values()
        size = len(dtlist)
        if size < 10:
            return False
        entrydf = pd.merge(entrydf, dybdf, on=['ts_code','trade_date'])

        curdt = entrydf['trade_date'].tail(1).get_values()[0]
        prvdt = entrydf['trade_date'].tail(2).get_values()[0]
        if curdt <'20190601':
            return False
        pmavar = (self.dystore.getMa(code, prvdt) - self.dystore.getMa(code,prvdt,10))/self.dystore.getMa(code,prvdt,10)
        cmavar = (self.dystore.getMa(code, curdt) - self.dystore.getMa(code,curdt,10))/self.dystore.getMa(code,curdt,10)
        am3chg = (numpy.average(entrydf['amount'].tail(10).tail(3)) - numpy.average(entrydf['amount'].tail(10).head(3)))/numpy.average(entrydf['amount'].tail(10).head(3))
        am2chg = (numpy.average(entrydf['amount'].tail(4).tail(2)) - numpy.average(entrydf['amount'].tail(4).head(2)))/numpy.average(entrydf['amount'].tail(4).head(2))
        am1chg = (entrydf['amount'].tail(1).get_values()[0] - numpy.average(entrydf['amount'].tail(5).head(4)))/numpy.average(entrydf['amount'].tail(5).head(4))
        cquery='trade_date==@curdt'
        pquery='trade_date==@prvdt'
        '''
        fmfentry = mfentry.assign(blgpct=(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol'])/(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol']+mfentry['buy_md_vol']+mfentry['buy_sm_vol']),slgpct=(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol'])/(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol']+mfentry['sell_md_vol']+mfentry['sell_sm_vol'])) 
        avgblgrt = numpy.average(fmfentry["blgpct"]) 
        stdblgrt = numpy.std(fmfentry["blgpct"])
        exslgrt = numpy.average(fmfentry["slgpct"]) + numpy.std(fmfentry["slgpct"])
        '''
        extort = numpy.average(entrydf["turnover_rate_f"]) + numpy.std(entrydf["turnover_rate_f"])
        avgmount = numpy.average(entrydf["amount"])
        stdmount = numpy.std(entrydf["amount"])
        clow = entrydf.query(cquery).get("low").get_values()[0]  
        phigh = entrydf.query(pquery).get("high").get_values()[0]
        chigh = entrydf.query(cquery).get("high").get_values()[0]
        copen = entrydf.query(cquery).get("open").get_values()[0]
        popen = entrydf.query(pquery).get("open").get_values()[0]
        cclose = entrydf.query(cquery).get("close").get_values()[0]  
        pclose = entrydf.query(pquery).get("close").get_values()[0]
        cchg1 = entrydf.query(cquery).get("pct_chg").get_values()[0]  
        pchg1 = entrydf.query(pquery).get("pct_chg").get_values()[0]  
        amount1 = entrydf.query(cquery).get("amount").get_values()[0]
        amount2 = entrydf.query(pquery).get("amount").get_values()[0]
        vol1 = entrydf.query(cquery).get("vol").get_values()[0]
        '''
        cblgpct = fmfentry.query(cquery).get("blgpct").get_values()[0]  
        pblgpct = fmfentry.query(pquery).get("blgpct").get_values()[0]         
        cslgpct = fmfentry.query(cquery).get("slgpct").get_values()[0]
        pslgpct = fmfentry.query(pquery).get("slgpct").get_values()[0] 
        cnetflow = fmfentry.query(cquery).get("net_mf_amount").get_values()[0]
        cnetvol = fmfentry.query(cquery).get("net_mf_vol").get_values()[0]
        '''
        ctf = entrydf.query(cquery).get("turnover_rate_f").get_values()[0]
        camchg = amount1/amount2
        if am1chg > 1.5 and camchg > 2 and (amount1 + amount2) > 160000 and cchg1 > 0 and cclose>6.4:
            results = self.checkpredict(code, curdt,5)
            if self.debug:
                print('strategy 1 - [' + code + '] and result: ' +str(results) + ',pay attendtion to amount change:' + str(camchg))
            self.saveanadb(code, curdt, 'strategy1', str(results))
            if not self.debug and (cchg1 + pchg1) <= 2:
                print('strategy 1X - [' + code + '] and result: ' +str(results) + ',pay attendtion to amount change:' + str(camchg))
            return True
        return False
    def calHigh(self, code, date=''):
        prvdt = None
        curdt = None
        entrydf =  self.dystore.loadEntry(code, date)
        mfentry = self.mfstore.loadEntry(code,date)
        if entrydf.empty:
            return False
        dtlist = entrydf['trade_date'].get_values()
        size = len(dtlist)
        if size < 10:
            return False
        curdt = entrydf['trade_date'].tail(1).get_values()[0]
        prvdt = entrydf['trade_date'].tail(2).get_values()[0]
        #print('curdt:' + curdt +' prvdt:' + prvdt)

        if curdt <'20190601':
            return False
        curma5 = self.dystore.getMa(code, curdt)
        curma10 = self.dystore.getMa(code, curdt,10)
        pmavar = self.dystore.getMa(code, prvdt) - self.dystore.getMa(code,prvdt,10)
        cmavar = self.dystore.getMa(code, curdt) - self.dystore.getMa(code,curdt,10)
        
        amchg = (numpy.average(entrydf['amount'].tail(10).tail(3)) - numpy.average(entrydf['amount'].tail(10).head(3)))/numpy.average(entrydf['amount'].tail(10).head(3))
        mamchg = (numpy.average(entrydf['amount'].tail(6).tail(3)) - numpy.average(entrydf['amount'].tail(6).head(3)))/numpy.average(entrydf['amount'].tail(6).head(3))
        if cmavar < 0 or pmavar < 0:
            if entrydf.query('pct_chg<=-5').empty:
                return False
        
        cquery='trade_date==@curdt'
        pquery='trade_date==@prvdt'
        #fmfentry = mfentry.assign(blgpct=(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol'])/(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol']+mfentry['buy_md_vol']+mfentry['buy_sm_vol']),slgpct=(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol'])/(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol']+mfentry['sell_md_vol']+mfentry['sell_sm_vol'])) 
        cchg1 = entrydf.query(cquery).get("pct_chg").get_values()[0]  
        pchg1 = entrydf.query(pquery).get("pct_chg").get_values()[0]  
        amount1 = entrydf.query(cquery).get("amount").get_values()[0]
        amount2 = entrydf.query(pquery).get("amount").get_values()[0]
        curclose = entrydf.query(cquery).get("close").get_values()[0]
        #cblgpct = fmfentry.query(cquery).get("blgpct").get_values()[0]  
        #pblgpct = fmfentry.query(pquery).get("blgpct").get_values()[0]         
        #cslgpct = fmfentry.query(cquery).get("slgpct").get_values()[0]
        #pslgpct = fmfentry.query(pquery).get("slgpct").get_values()[0] 
        #cnetflow = mfentry.query(cquery).get("net_mf_amount").get_values()[0]
        #ccmavar = (curclose - curma10)/curma10
        if amchg < 2.5:
            if amchg < 1:
                return False
            if  cchg1 < 5 and pchg1 < 5 and (pchg1+cchg1)>-1  and (amount1 + amount2) > 120000 and mamchg>0.5:
                print('strategy 2- [' + code + ']')
                return True
            if cchg1 < -5  and (pchg1+cchg1)>-2  and (amount1 + amount2) > 120000:
                print('strategy 3- [' + code + ']')
            return True
        camchg = (entrydf['amount'].tail(1).get_values()[0] - numpy.average(entrydf['amount'].tail(4).head(3)))/numpy.average(entrydf['amount'].tail(4).head(3))

        if  camchg>5:
            print('strategy 4 - [' + code + ']')
        return True
        '''
        if (cnetflow > 0 and cblgpct > 0.33  \
            and cchg1 < 3 and pchg1 < 3 and (pchg1+cchg1)>-0.5 \
            and (amount1 + amount2) > 100000) or \
            (cnetflow < 0 and cslgpct < 0.3  \
            and cchg1 <3 and pchg1<3 and (pchg1+cchg1)>-0.5 \
            and (amount1 + amount2) > 100000):
            print('[' + code + ']' )
        '''
    def generateStockInfo(self, code, date):
        dyentry = self.dystore.loadData(code, date, date)
        if dyentry.empty:
            return dyentry
        bchg = dyentry.query('pct_chg>=8')
        if bchg.empty:
            return bchg
        dt = bchg['trade_date'].get_values()[0]
        ent = self.dystore.loadEntry(code, date=dt, line = 5)
        mfent = self.mfstore.loadEntry(code, date=dt, line = 5)
        mfent = mfent.assign(blgpct=(mfent['buy_elg_vol'] + mfent['buy_lg_vol'])/(mfent['buy_elg_vol'] + mfent['buy_lg_vol']+mfent['buy_md_vol']+mfent['buy_sm_vol']),\
            slgpct=(mfent['sell_elg_vol'] + mfent['sell_lg_vol'])/(mfent['sell_elg_vol'] + mfent['sell_lg_vol']+mfent['sell_md_vol']+mfent['sell_sm_vol']))

        newpd = pd.merge(ent.filter(items=['ts_code', 'trade_date','open','close','high','low', 'pct_chg','amount','vol']),\
            mfent.filter(items=['ts_code', 'trade_date', 'blgpct','slgpct','net_mf_vol']), \
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
    
    def gendstk(self, code, date):
        dyoffset = 2
        dyentry = self.dystore.getrecInfo(code, date, dyoffset)
        if dyentry.empty:
            return dyentry
        dt = dyentry['trade_date'].get_values()[0]
        ent = self.dystore.loadEntry(code, date=dt, line = 5)
        mfent = self.mfstore.loadEntry(code, date=dt, line = 5)
        mfent = mfent.assign(blgpct=(mfent['buy_elg_vol'] + mfent['buy_lg_vol'])/(mfent['buy_elg_vol'] + mfent['buy_lg_vol']+mfent['buy_md_vol']+mfent['buy_sm_vol']),\
            slgpct=(mfent['sell_elg_vol'] + mfent['sell_lg_vol'])/(mfent['sell_elg_vol'] + mfent['sell_lg_vol']+mfent['sell_md_vol']+mfent['sell_sm_vol']))

        newpd = pd.merge(ent.filter(items=['ts_code', 'trade_date','open','close','high','low', 'pct_chg','amount','vol']),\
            mfent.filter(items=['ts_code', 'trade_date', 'blgpct','slgpct','net_mf_vol','net_mf_amount']), \
                on=['ts_code','trade_date'])
        ffdt = newpd['trade_date'].get_values()
        madf = None
        for fdt in ffdt:
            tment = self.mfstore.loadEntry(code, date=fdt, line = 90)
            tment = tment.assign(blgpct=(tment['buy_elg_vol'] + tment['buy_lg_vol'])/(tment['buy_elg_vol'] + tment['buy_lg_vol']+tment['buy_md_vol']+tment['buy_sm_vol']),\
                slgpct=(tment['sell_elg_vol'] + tment['sell_lg_vol'])/(tment['sell_elg_vol'] + tment['sell_lg_vol']+tment['sell_md_vol']+tment['sell_sm_vol']))
            entrydf = self.dystore.loadEntry(code, date=fdt, line = 90)
            ma5 = self.dystore.getMa(code, fdt)
            ma10 = self.dystore.getMa(code, fdt,10)
            vamount = self.dystore.getAvg(code,'amount',fdt, 90) +  self.dystore.getStd(code,'amount',fdt, 90)
            vavgblgpct = numpy.average(tment['blgpct'])  
            vstdblgpct = numpy.std(tment['blgpct'])
            vavgslgpct = numpy.average(tment['slgpct']) 
            vstdslgpct = numpy.std(tment['slgpct'])            
            amchg = (numpy.average(entrydf['amount'].tail(4).tail(2)) - numpy.average(entrydf['amount'].tail(4).head(2)))/numpy.average(entrydf['amount'].tail(4).head(2))
            vavgpct = numpy.average(entrydf.tail(3)['pct_chg'])
            vavgflow = numpy.average(tment.tail(3)['net_mf_amount'])
            dicitem = {'ts_code': stockInfoStore.canoncode(code), 'trade_date': fdt, 'ma5': ma5, 'ma10': ma10, \
                'vamount':vamount, 'vavgblgpct':vavgblgpct,'vstdblgpct':vstdblgpct,\
                'vavgslgpct':vavgslgpct,'vstdslgpct':vstdslgpct, 'vavgpct':vavgpct, 'vavgflow':vavgflow,\
                'amchg':amchg }
            if madf is None:
                madf = pd.DataFrame.from_records(dicitem,index=[0])
            else:
                madf = madf.append(pd.DataFrame.from_records(dicitem,index=[0]))
        newpd = pd.merge(newpd, madf,  on=['ts_code','trade_date'])
        return newpd
    
    
    def generateStockAllInfo(self, code, ln=120):
        ent = self.dystore.loadEntry(code, line=ln)
        if ent.empty:
            return
        mfent = self.mfstore.loadEntry(code, line = ln)
        mfent = mfent.assign(blgpct=(mfent['buy_elg_vol'] + mfent['buy_lg_vol'])/(mfent['buy_elg_vol'] + mfent['buy_lg_vol']+mfent['buy_md_vol']+mfent['buy_sm_vol']),\
            slgpct=(mfent['sell_elg_vol'] + mfent['sell_lg_vol'])/(mfent['sell_elg_vol'] + mfent['sell_lg_vol']+mfent['sell_md_vol']+mfent['sell_sm_vol']))

        newpd = pd.merge(ent.filter(items=['ts_code', 'trade_date','open','close','high','pct_chg','amount','vol']),\
            mfent.filter(items=['ts_code', 'trade_date', 'blgpct','slgpct','net_mf_vol', 'net_mf_amount']), \
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
        newpd.to_excel('cal_' +code + '.xlsx')

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
    def testrule(self, date=''):
        rdate = date
        thresh=5
        if date == '':
            rdate = datetime.datetime.now().strftime('%Y%m%d')
        df = self.bgstock.loadallstock()
        mdf = None
        print('start')
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            if not self.pdavailable(code, rdate) or not self.checkright(code, rdate,thresh):
                continue
            if not self.calrate(code,rdate):
                print('rule not applied for [' + code +']') 
        print('done')

    def genstduyhgstock(self, date=''):
        rdate = date
        thresh=5
        if date == '':
            rdate = datetime.datetime.now().strftime('%Y%m%d')
        df = self.bgstock.loadallstock()
        mdf = None
        print('start')
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            if not self.pdavailable(code, rdate) or not self.checkright(code, rdate,thresh):
                continue
            tmpdf = self.gendstk(code,rdate)
            if tmpdf.empty:
                continue
            if mdf is None:
                mdf = tmpdf
            else:
                mdf = mdf.append(tmpdf)
            print("code:"+ code + " done")
        if mdf is None:
            print("not available for " + rdate)
            return False
        mdf.to_excel('validate' + rdate + '.xlsx')
        print('done')
    
    def genhgstockinfo(self, date=''):
            rdate = date
            if date == '':
                rdate = datetime.datetime.now().strftime('%Y%m%d')
            df = self.bgstock.loadallstock()
            mdf = None
            print('start')
            for i in range(0, df.index.size):
                bstockdic = df.iloc[i].to_dict()
                code = bstockdic['symbol']
                tmpdf = self.generateStockInfo(code,rdate)
                if tmpdf.empty:
                    continue
                if mdf is None:
                    mdf = tmpdf
                else:
                    mdf = mdf.append(tmpdf)
                print("code:"+ code + " done")
            mdf.to_excel('allhigh' + rdate + '.xlsx')
            print('done')
        
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
    def findsuit(self, date=''):
        rdate = date
        if date == '':
            rdate = datetime.datetime.now().strftime('%Y%m%d')
        df = self.bgstock.loadallstock()
        resdf = None
        vv=0
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            if self.calrate(code, rdate) and self.debug:
                dicitem=self.genresultfld(code, rdate)
                if resdf is None:
                    resdf = pd.DataFrame.from_records(dicitem,index=[vv])
                else:
                    resdf = resdf.append(pd.DataFrame.from_records(dicitem,index=[vv]), sort=False)
                vv = vv + 1
        if not resdf is None:
            resdf.to_excel('findsuitinfo' + rdate + '.xlsx')

            #self.calHigh(code, rdate)    
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