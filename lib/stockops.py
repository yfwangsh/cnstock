from pymongo import MongoClient
import tushare as ts
import pandas as pd
import numpy
import datetime
import unittest
import requests
import json
import hmac
import hashlib
import time
import base64
import urllib
import sys
import os
from enum import Enum

sys.path.append(os.getcwd() + '/lib')
sys.path.append(os.getcwd())
from lib.datastore import *
from lib.utils import func_line_time


class polltype(Enum):
    other = 0
    crs = 1
    crszu = 2
    crszd = 3 
    crsup = 4
    crsdown = 5
    zline = 6
    full = 7
    ulong = 8
    dlong = 9
    zzdlong = 10
    zzd = 11
    zzu = 12
    zzulong = 13


class pollst:
    def __init__(self, code, trade_date):
        self.upline = 0
        self.downline = 0
        self.neg = False
        self.postline = 0
        self.pct_chg = 0
        self.jump = False
        self.flag = None
        self.openp = 0
        self.closep = 0
        self.highp = 0
        self.lowp = 0
        self.code = code
        self.trade_date = trade_date
        self.prev = None
        self.next = None
        self.vol = 0
        self.amount = 0

    def buildpost(self, openp, closep, highp, lowp, pct_chg, vol, amount):
        self.openp = openp
        self.closep = closep
        self.highp = highp
        self.lowp = lowp
        self.pct_chg = pct_chg
        self.vol = vol
        self.amount = amount
        self.upline = round((highp - max(openp, closep))/closep * 100, 2)
        self.downline = round((min(openp, closep) - lowp)/closep * 100, 2)
        self.postline = round(abs(openp - closep)/closep*100, 2)
        self.frame = round((highp - lowp)/closep * 100, 2)
        self.pct_chg = pct_chg
        self.neg = (openp - closep) > 0
        self.jump = pct_chg > 0 and (lowp - closep/(1+pct_chg/100))>0 

    def buildfromsi(self, si):
        openp = si['open']
        closep = si['close']
        highp = si['high']
        lowp = si['low']
        pct_chg = si['pct_chg']
        vol = si['vol']
        amount = si['amount']
        self.buildpost(openp, closep, highp, lowp, pct_chg, vol, amount)
    
    def buildfromdf(self, df):
        openp = df['open'].to_numpy()[0]
        closep = df['close'].to_numpy()[0]
        highp = df['high'].to_numpy()[0]
        lowp = df['low'].to_numpy()[0]
        pct_chg = df['pct_chg'].to_numpy()[0]
        vol = df['vol'].to_numpy()[0]
        amount = df['amount'].to_numpy()[0]
        self.buildpost(openp, closep, highp, lowp, pct_chg, vol, amount)
    def getNode(self, date):
        if self.trade_date == date:
            return self
        elif self.trade_date > date:
            node = self.prev
            while  node is not None:
                if node.trade_date == date:
                    return node
                node = node.prev
                if node.trade_date < date:
                    return None
        elif self.trade_date < date:
            node = self.next
            while node is not None:
                if node.trade_date == date:
                    return node
                node = node.next
                if node.trade_date > date:
                    return node
        return None       


    @staticmethod
    def mcalt(num):
        w1 = 'T'
        if num < 1:
            pass
        elif num <= 2.5:
            w1 = 'L'
        elif num <= 5:
            w1 = 'M'
        elif num <= 8:
            w1 = 'H'
        elif num <= 11:
            w1 = 'G'
        else:
            w1 = 'E'
        return w1    
    @staticmethod
    def calt(num):
        w1 = 0
        if num < 1:
            w1 = 0
        elif num <= 3:
            w1 = 1
        elif num <= 5:
            w1 = 2
        elif num < 9:
            w1 = 3
        else:
            w1 = 4
        return w1
    @staticmethod 
    def calpct(var, base, absflag=False):
        ret  = (var - base)/base * 100
        if absflag:
            ret = abs(ret)
        return round(ret,3) 
    def getzPoint(self):
        return (self.openp + self.closep)/2
    def stype(self):
        if self.frame < 0.9 or (self.upline < 0.5 and self.downline < 0.5 and self.postline < 0.5):
            return polltype.zline
        if self.postline < 0.5:
            if self.upline <= self.postline:
                return polltype.crszd
            if self.downline <= self.postline:
                return polltype.crszu
            if self.upline > self.downline + 0.3:
                return polltype.crsup
            if self.downline > self.upline + 0.3:
                return polltype.crsdown
            return polltype.crsdown
        else:
            if self.upline < 0.5:
                if self.downline <= self.upline:
                    return polltype.full
                if self.downline > self.postline + 1:
                    return polltype.zzdlong
                return polltype.zzd
            if self.downline < 0.5:
                if self.upline <= self.downline:
                    return polltype.full
                if self.upline > self.postline + 1:
                    return polltype.zzulong
                return polltype.zzu            
        return polltype.other
    def getpolltype(self):
        return pollst.mcalt(self.pct_chg) + ':' + str(self.stype()) + ':' + pollst.mcalt(self.frame)
    def getType(self):
        if self.flag is None:
            self.flag = str(self.calt(self.upline)) + str(self.calt(self.postline))+ str(self.calt(self.downline))
            if self.neg:
                self.flag = '-' + self.flag
            else:
                self.flag = '+' + self.flag
            if self.pct_chg > 0:
                self.flag = self.flag + '+' + str(self.calt(self.pct_chg))
            else:
                self.flag = self.flag + '-' +  str(self.calt(abs(self.pct_chg)))
            if self.prev is not None:
                hgpct = self.calpct((self.openp+self.closep)/2, (self.prev.openp + self.prev.closep)/2)
                if hgpct > 0:
                    self.flag = self.flag + '+'
                else:
                    self.flag = self.flag + '-'
                self.flag = self.flag + str(self.calt(abs(hgpct)))
        return self.flag
    



class DingTalk_Base:
    def __init__(self):
        self.__headers = {'Content-Type': 'application/json;charset=utf-8'}
        self.url = ''
        self.secret = ''
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
        if len(self.secret) == 0:
            return requests.post(self.url, json.dumps(json_text), headers=self.__headers).content
        timestamp = round(time.time()*1000)
        secret_enc = bytes(self.secret.encode('utf-8'))
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = bytes(string_to_sign.encode('utf-8'))
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        signurl = self.url + '&timestamp=' + str(timestamp) + '&sign=' + sign
        return requests.post(signurl, json.dumps(json_text), headers=self.__headers).content
        
class DingTalk(DingTalk_Base):
    def __init__(self):
        super().__init__()
        # 填写机器人的url
        self.url = ''
    def setURL(self, url):
        self.url = url
    def initSec(self, secret):
        self.secret = secret

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
    def getDBRange(self, bg=True):
        start, end = self.getStoreTMRange('SSE')
        if bg:
            return start
        else:
            return end
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
        if self.isTrade(today) == 1:
            nowtime = datetime.datetime.now()
            amtmstart =datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 9,20,0,0)
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
        high = df1.get("high").to_numpy()[0]
        low = df1.get("low").to_numpy()[0]
        '''
        开盘价和收盘价
        '''
        closep = df1.get("close").to_numpy()[0]
        openp = df1.get("open").to_numpy()[0]
        
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
        
        knowopdic['highstart'] = df2['time'].head(1).to_numpy()[0]
        knowopdic['highend'] = df2['time'].tail(1).to_numpy()[0]
        knowopdic['lowstart'] = df3['time'].head(1).to_numpy()[0]
        knowopdic['lowend'] = df3['time'].tail(1).to_numpy()[0]
        tmhighend = knowopdic['highend'] 
        tmlowstart = knowopdic['lowstart']
        df5 = df.query('time>=@tmhighend').sort_values(by=['price'])
        df6 = df.query('time<=@tmlowstart').sort_values(by=['price'])
        knowopdic['highendtolow'] = df5['price'].head(1).to_numpy()[0]
        knowopdic['lowstartbeforehigh'] = df6['price'].tail(1).to_numpy()[0]
        trendstr = stockInfoStore.removeDups(df4['type'].to_numpy())
        ndtradeinfo = self.dystore.getrecInfo(code, trade_date, 1)
        if ndtradeinfo is None:
            return None
        nxhigh = ndtradeinfo.get("high").to_numpy()[0]
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
        self.conf = Singleton()
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
        return df[['code','name','open','high','low','pre_close','price','bid','ask','volume','amount','time']]
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
    def rtupdateUpmonSet(self, dt=None):
        trade_date = dt
        if dt is None: 
            trade_date = datetime.datetime.now().strftime('%Y%m%d')
        dtflag = self.tradedate.isTrade(trade_date) 
        if not dtflag == 1:
            return
        lasttrade = self.tradedate.getlasttrade(trade_date)
        df = self.getUpMonSet(lasttrade)
        for rr in df.itertuples(index=False):
            mydict = rr._asdict()
            code = mydict['code']
            date = mydict['trade_date']
            price = mydict['Pricetarget']
            edf = self.getRealQuote(code)
            name = edf['name'].to_numpy()[0]
            openp = float(edf['open'].to_numpy()[0])
            if (openp - price)/price > 0.005:
                self.analyst.setMonitoring(code, date, price, flag=True, rule='strategy2')               
            else:
                self.analyst.setMonitoring(code, date, price, flag=False, rule='strategy2')


    def updateUpmonSet(self, date):
        df = self.getUpMonSet(date)
        if df is None or df.empty:
            return
        for rr in df.itertuples(index=False):
            mydict = rr._asdict()
            code = mydict['code']
            date = mydict['trade_date']
            price = mydict['Pricetarget']
            basenode = self.buildposts(code, date, node=2)
            if basenode.next is not None:
                if (basenode.next.openp - price)/price > 0.005:
                    self.analyst.setMonitoring(code, date, price, flag=True, rule='strategy2')               
                else:
                    self.analyst.setMonitoring(code, date, price, flag=False, rule='strategy2')

    def updateMonitorSetByDay(self, start_date, end_date):
        df = self.analyst.fetchDataEntry(start_date, end_date)
        for rr in df.itertuples(index=False):
            mydict = rr._asdict()
            code = mydict['code']
            date = mydict['trade_date']
            if mydict.__contains__('mpflag') and mydict['mpflag'] == 1:
                continue
            centry = self.dystore.loadEntry(code, line=1)
            basenode = self.buildposts(code, date, node=10)
            PriceTarget = self.getProposed(code, date)
            umonitor = False
            cpnode = basenode.next
            mvday = 1
            while cpnode is not None:
                if pollst.calpct(cpnode.lowp, PriceTarget) <= -2:
                    self.analyst.setMonitoring(code, date, PriceTarget, flag=False)
                    umonitor = True
                    break
                if pollst.calpct(cpnode.closep, PriceTarget) >= 15 and mvday >= 3:
                    #todo adjust monitor price
                    self.analyst.setMonitoring(code, date, PriceTarget, flag=False)
                    umonitor = True
                    break
                cpnode = cpnode.next
                mvday = mvday + 1
            if not umonitor:
                 self.analyst.setMonitoring(code, date, PriceTarget)
    def updateMonitorSet(self, line=12):
        trade_date = datetime.datetime.now().strftime('%Y%m%d')
        lasttrade = trade_date
        dtflag = self.tradedate.isTrade(trade_date) 
        if dtflag == 0:
            lasttrade = self.tradedate.getlasttrade(trade_date)
        elif dtflag == -1:
            lasttrade = self.tradedate.getDBRange(bg=False)
        count = 1
        while count < line:
            count = count + 1
            lasttrade = self.tradedate.getlasttrade(lasttrade)
        self.updateMonitorSetByDay(lasttrade, trade_date)
    def getMonitorSet(self, line=12):
        trade_date = datetime.datetime.now().strftime('%Y%m%d')
        lasttrade = trade_date
        dtflag = self.tradedate.isTrade(trade_date)
        if self.tradedate.isTrade(trade_date) == 0:
            lasttrade = self.tradedate.getlasttrade(trade_date)
        elif dtflag == -1:
            lasttrade = self.tradedate.getDBRange(bg=False)
        count = 1
        while count < line:
            count = count + 1
            lasttrade = self.tradedate.getlasttrade(lasttrade)
        df = self.analyst.fetchDataEntry(lasttrade)
        if 'Monitorflag' in df.columns:
            if 'fmflag' in df.columns:
                return df.query('Monitorflag==1 or fmflag==1')
            else:
                return df.query('Monitorflag==1')
        else:
            if df.empty:
                return df
            return None
    def getMonitorDaySet(self, date):
        df = self.analyst.loadData(date)
        if 'Monitorflag' in df.columns:
            if 'fmflag' in df.columns:
                return df.query('Monitorflag==1 or fmflag==1')
            else:
                return df.query('Monitorflag==1')
        else:
            if df.empty:
                return df
            return None

    def checkpredict(self,code, date,thresh=2.5):
        entry = self.dystore.loadEntry(code, date, line=12, prv=False)
        entsize = len(entry['trade_date'].to_numpy())
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
        tptmax = maxptentry['close'].to_numpy()[0]
        dt = maxentry['trade_date'].to_numpy()[0]
        pdt = maxptentry['trade_date'].to_numpy()[0]
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
        lowdt = minentry['trade_date'].to_numpy()[0]
        lowpdt = minptentry['trade_date'].to_numpy()[0]

        rpct = round((tpmax - lowmin)/lowmin * 100, 2)
        rpcdt = round((tptmax-lowptmin)/lowptmin *100, 2)
        if rpct >= thresh or rpcdt >= thresh:
            return True
        return False

    
    def saveanadb(self, code, trade_date, matchRule, predresult, price=0, monflag=None):
        #df = self.dystore.getrecInfo(code, trade_date, offset=0)
        entrydic = {}
        entrydic['code'] = code
        entrydic['trade_date'] = trade_date
        entrydic['match_rule'] = matchRule
        entrydic['Pricetarget'] = price
        if predresult is not None:
            entrydic['result']  = predresult
        if monflag is not None:
            monitor = 0
            if monflag:
                monitor = 1
            entrydic['Monitorflag'] = monitor
        entrydic['lastupdated'] = datetime.datetime.now().strftime('%Y%m%d')
        self.analyst.saveRec(entrydic)


    def getProposed(self, code, trade_date):
        retval = None
        df = self.dystore.getrecInfo(code, trade_date, offset=0)
        dfprev = self.dystore.getrecInfo(code, trade_date, offset=-1)
        dfpprev = self.dystore.getrecInfo(code, trade_date, offset=-2)
        if df is None or dfprev is None or dfpprev is None:
            return retval
        cchg = df['pct_chg'].to_numpy()[0]
        pchg = dfprev['pct_chg'].to_numpy()[0]
        
        if (cchg+pchg)>=15:
            retval = round(df['low'].to_numpy()[0] * (100-cchg)/100, 2)
            return retval

        if (cchg+pchg)<15 and (cchg+pchg)>=11:
            retval = round(df['close'].to_numpy()[0] * (100-(cchg+pchg)/2)/100, 2)
            return retval

        if cchg > 0 and pchg <= 4 and (cchg+pchg)<11:
            mnprice = min(dfprev['open'].to_numpy()[0],dfprev['close'].to_numpy()[0]) 
            hgprice = max(dfprev['open'].to_numpy()[0],dfprev['close'].to_numpy()[0])
            if (pchg > 0 or (pchg < 0 and cchg >9.9)) and (cchg+pchg) >= 3 :  
                retval = round((dfprev['high'].to_numpy()[0]+hgprice)/2, 2) 
            else:
                retval = round((dfprev['low'].to_numpy()[0]+mnprice)/2, 2) 
            return retval 
        
        if cchg > 0 and pchg > 4 and (cchg+pchg)<11:
            if pchg < 9:
                hgprice = max(dfpprev['open'].to_numpy()[0],dfpprev['close'].to_numpy()[0])  
                retval = round(hgprice, 2) 
            else:
                retval = round(df['close'].to_numpy()[0]* (100 - (cchg+pchg)/2)/100, 2)
            return retval 
    #@func_line_time
    def calrate(self, code, date=''):
        if date == '':
            date = datetime.datetime.now().strftime('%Y%m%d')
        df = self.dystore.getrecInfo(code, date, offset=0)
        if df is None:
            return False
        entrydf =  self.dystore.loadEntry(code, date, 15)
        dybdf = self.dybstore.loadEntry(code, date, 15)
        #mfentry = self.mfstore.loadEntry(code, date, 15)
        if entrydf.empty:
            return False
        entrydf = pd.merge(entrydf, dybdf, on=['ts_code','trade_date'])
        entrydf = entrydf.sort_values(by='trade_date', ascending = False)
        dtlist = entrydf['trade_date'].to_numpy()
        size = len(dtlist)
        '''
        新股不考虑
        '''
        if size < 15:
            return False
        #dtlist.sort()
        #dtlist = dtlist[::-1]
        curdt = dtlist[0]
        #prvdt = dtlist[1]
        if (datetime.datetime.strptime(date,'%Y%m%d') - datetime.datetime.strptime(curdt,'%Y%m%d')) > 10*datetime.timedelta(days=1):
            return False
        #pmavar = (self.dystore.getMa(code, prvdt) - self.dystore.getMa(code,prvdt,10))/self.dystore.getMa(code,prvdt,10)
        #cmavar = (self.dystore.getMa(code, curdt) - self.dystore.getMa(code,curdt,10))/self.dystore.getMa(code,curdt,10)
        #am3chg = (numpy.average(entrydf['amount'].tail(10).tail(3)) - numpy.average(entrydf['amount'].tail(10).head(3)))/numpy.average(entrydf['amount'].tail(10).head(3))
        #am2chg = (numpy.average(entrydf['amount'].tail(4).tail(2)) - numpy.average(entrydf['amount'].tail(4).head(2)))/numpy.average(entrydf['amount'].tail(4).head(2))

        '''
        cquery='trade_date==@curdt'
        pquery='trade_date==@prvdt'
        fmfentry = mfentry.assign(blgpct=(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol'])/(mfentry['buy_elg_vol'] + mfentry['buy_lg_vol']+mfentry['buy_md_vol']+mfentry['buy_sm_vol']),slgpct=(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol'])/(mfentry['sell_elg_vol'] + mfentry['sell_lg_vol']+mfentry['sell_md_vol']+mfentry['sell_sm_vol'])) 
        avgblgrt = numpy.average(fmfentry["blgpct"]) 
        stdblgrt = numpy.std(fmfentry["blgpct"])
        exslgrt = numpy.average(fmfentry["slgpct"]) + numpy.std(fmfentry["slgpct"])
        '''
        '''
        extort = numpy.average(entrydf["turnover_rate_f"]) + numpy.std(entrydf["turnover_rate_f"])
        avgmount = numpy.average(entrydf["amount"])
        stdmount = numpy.std(entrydf["amount"])
        clow = entrydf["low"].to_numpy()[0]  
        phigh = entrydf["high"].to_numpy()[1]
        chigh = entrydf["high"].to_numpy()[0]
        copen = entrydf["open"].to_numpy()[0]
        popen = entrydf["open"].to_numpy()[1]
        pclose = entrydf["close"].to_numpy()[1]
        vol1 = entrydf["vol"].to_numpy()[0]
        '''
        amountlist = entrydf['amount'].to_numpy()
        am1chg = (amountlist[0] - numpy.average(amountlist[1:5]))/numpy.average(amountlist[1:5])
        cclose = entrydf["close"].to_numpy()[0]  
        cchg1 = entrydf["pct_chg"].to_numpy()[0]  
        pchg1 = entrydf["pct_chg"].to_numpy()[1]  
        amount1 = amountlist[0]
        amount2 = amountlist[1]
        '''
        cblgpct = fmfentry.query(cquery).get("blgpct").to_numpy()[0]  
        pblgpct = fmfentry.query(pquery).get("blgpct").to_numpy()[0]         
        cslgpct = fmfentry.query(cquery).get("slgpct").to_numpy()[0]
        pslgpct = fmfentry.query(pquery).get("slgpct").to_numpy()[0] 
        cnetflow = fmfentry.query(cquery).get("net_mf_amount").to_numpy()[0]
        cnetvol = fmfentry.query(cquery).get("net_mf_vol").to_numpy()[0]
        '''
        camchg = amount1/amount2

        if am1chg > 1.5 and camchg > 2 and (amount1 + amount2) > 160000 and cchg1 > 0 and cclose>6.4:
            if (cchg1 + pchg1) <= 3 and pchg1 > 0:
                return False
            #results = self.checkpredict(code, curdt,5)
            print('strategy 1 - @%s : [%s] pay attendtion to amount change: %s'%(curdt, code, str(camchg)))
            self.saveanadb(code, curdt, 'strategy1', None)
            return True
        return False
    def calHigh(self, code, date=''):
        prvdt = None
        curdt = None
        entrydf =  self.dystore.loadEntry(code, date)
        mfentry = self.mfstore.loadEntry(code,date)
        if entrydf.empty:
            return False
        dtlist = entrydf['trade_date'].to_numpy()
        size = len(dtlist)
        if size < 10:
            return False
        curdt = entrydf['trade_date'].tail(1).to_numpy()[0]
        prvdt = entrydf['trade_date'].tail(2).to_numpy()[0]
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
        cchg1 = entrydf.query(cquery).get("pct_chg").to_numpy()[0]  
        pchg1 = entrydf.query(pquery).get("pct_chg").to_numpy()[0]  
        amount1 = entrydf.query(cquery).get("amount").to_numpy()[0]
        amount2 = entrydf.query(pquery).get("amount").to_numpy()[0]
        curclose = entrydf.query(cquery).get("close").to_numpy()[0]
        #cblgpct = fmfentry.query(cquery).get("blgpct").to_numpy()[0]  
        #pblgpct = fmfentry.query(pquery).get("blgpct").to_numpy()[0]         
        #cslgpct = fmfentry.query(cquery).get("slgpct").to_numpy()[0]
        #pslgpct = fmfentry.query(pquery).get("slgpct").to_numpy()[0] 
        #cnetflow = mfentry.query(cquery).get("net_mf_amount").to_numpy()[0]
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
        camchg = (entrydf['amount'].tail(1).to_numpy()[0] - numpy.average(entrydf['amount'].tail(4).head(3)))/numpy.average(entrydf['amount'].tail(4).head(3))

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
    @staticmethod
    def getAllRT():
        df = ts.get_today_all()
        return df
    def generateStockInfo(self, code, date):
        dyentry = self.dystore.loadData(code, date, date)
        if dyentry.empty:
            return dyentry
        bchg = dyentry.query('pct_chg>=8')
        if bchg.empty:
            return bchg
        dt = bchg['trade_date'].to_numpy()[0]
        ent = self.dystore.loadEntry(code, date=dt, line = 5)
        mfent = self.mfstore.loadEntry(code, date=dt, line = 5)
        mfent = mfent.assign(blgpct=(mfent['buy_elg_vol'] + mfent['buy_lg_vol'])/(mfent['buy_elg_vol'] + mfent['buy_lg_vol']+mfent['buy_md_vol']+mfent['buy_sm_vol']),\
            slgpct=(mfent['sell_elg_vol'] + mfent['sell_lg_vol'])/(mfent['sell_elg_vol'] + mfent['sell_lg_vol']+mfent['sell_md_vol']+mfent['sell_sm_vol']))

        newpd = pd.merge(ent.filter(items=['ts_code', 'trade_date','open','close','high','low', 'pct_chg','amount','vol']),\
            mfent.filter(items=['ts_code', 'trade_date', 'blgpct','slgpct','net_mf_vol']), \
                on=['ts_code','trade_date'])
        ffdt = newpd['trade_date'].to_numpy()
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
    def addCMonitor(self, code, trade_date=None):
        curdt = trade_date
        cpo = None
        df = self.getCMonitorbyCode(code)
        if not df.empty:
            return
        if trade_date is None:
            curdt = datetime.datetime.now().strftime('%Y%m%d')
        while cpo is None:
            cpo = self.buildposts(code, curdt, node = 2)
            curdt = self.tradedate.getlasttrade(curdt)
        self.saveanadb(code, curdt, 'strategy3', 'True', cpo.highp, True)

    def updateCMonitor(self, code, price, monflag=True):
        date = datetime.datetime.now().strftime('%Y%m%d')
        self.analyst.setMonitoring(code, date, price, flag=monflag, rule='strategy3', dtkey=False)
    def getCMonitor(self):
        df = self.analyst.loadData(trade_date='', rule='strategy3', dtkey=False)
        return df 
    def getCMonitorbyCode(self, code):
        df = self.analyst.fetchDataEntryByCode(code, rule='strategy3')
        return df 

    def calup(self, code, date):
        cpo = self.buildposts(code, date, node = 7)
        if cpo is None or cpo.closep<=7 or cpo.pct_chg > 5:
            return False
        stdbr = cpo.getzPoint()
        baser = stdbr
        ppo = cpo.prev
        mv = 0
        cflag = False
        vcount = 0
        while ppo is not None and vcount <=3:
            if vcount == 2:
                cflag = True
            
            if ppo.getzPoint() < baser:
                vcount = vcount + 1
            if vcount > 2:
                break
            if vcount == 2 and cflag == True:
                cflag = False
                break
            if ppo.pct_chg > 9.9 or ppo.neg:
                cflag = True
                break
            
            baser = ppo.getzPoint()
            iclose = ppo.closep
            ppo = ppo.prev
            mv = mv + 1
            if mv > vcount:
                cflag = True
                break
        if cflag or cpo.pct_chg <= -0.5:
            return False
        if (cpo.closep - iclose)/iclose < 0.02 or (cpo.closep - iclose)/iclose > 0.08:
            return  False
        if cpo.upline >= 1.6:
            return False
        self.saveanadb(code, date, 'strategy2', 'True', stdbr)

    def getUpMonSet(self, trade_date = None):
        curdt = trade_date
        if trade_date is None:
            curdt = datetime.datetime.now().strftime('%Y%m%d')
        df = self.analyst.loadData(trade_date=curdt, rule='strategy2')
        return df
    #@func_line_time
    def getstdV(self, arrays):
        namrry = numpy.sort(arrays)    
        idx = 0
        lastrt = -1
        for i in range(1, int(len(arrays)/2)):
            st = namrry[i]
            ed = namrry[len(arrays) - i]
            if ed/st > 2:
                continue
            idx = i
            break
        if idx <= 1:
            return (0,numpy.average(namrry), 0)
        return (numpy.average(namrry[0:idx-1]), numpy.average(namrry[idx: len(namrry) - idx]), numpy.average(namrry[len(namrry)-idx+1:]))
    def getAmountInfo(self, code, rg=60):
        ### 量减少 收红 阳线 考虑入， 阴线考虑出
        retarry = [code]
        entry = self.dystore.loadEntry(code, line=rg)
        if entry is None:
            return None
        amentry = entry[['amount']].sort_values(by='amount')
        namrry = amentry.get('amount').to_numpy()
        dentry = self.dybstore.loadEntry(code, line=rg)
        avcmv = numpy.average(dentry.get('circ_mv').to_numpy())
        avtmv = numpy.average(dentry.get('total_mv').to_numpy())
        aminfo =  self.getstdV(namrry)
        retarry.append(avcmv)
        retarry.append(avtmv)
        retarry.append(aminfo[0])
        retarry.append(aminfo[1])
        retarry.append(aminfo[2])
        return retarry
    def actionp(self, code, date, rc=True):
        action= 0
        cnode = self.buildposts(code, date, node=3, both=-1)
        if cnode is None:
            return action
        if cnode.prev is None:
            return action
        if cnode.prev.trade_date != self.tradedate.getlasttrade(cnode.trade_date):
            return action
        amchg = pollst.calpct(cnode.amount, cnode.prev.amount)
        oprate = pollst.calpct(cnode.closep, cnode.openp)
        hrate = pollst.calpct(cnode.highp, cnode.prev.highp)
        przrate = (cnode.prev.highp - cnode.prev.lowp)/cnode.prev.closep * 100
        if rc :
            if oprate > 0.5 and cnode.pct_chg > 0.5 and amchg < -33.3 and \
                cnode.prev.pct_chg > 0 and not cnode.neg and not cnode.prev.neg and \
                    hrate > 0.5:
                action = 1
            rprv = self.actionp(code, cnode.prev.trade_date, rc = False)
            if action == 1 and rprv == 1:
                action = -1
        else:
            if cnode.pct_chg > 0.5 and amchg < -20 and cnode.prev.pct_chg > 0 and not cnode.neg and not cnode.prev.neg:
                action = 1
        if rc and not action == 0:
            if cnode.pct_chg > 9 or cnode.prev.pct_chg > 9:
                action = 0
            self.saveanadb(code, date, 'strategy4', 'True', action) 
        return action
    def genamount(self, outfile='stockamount', rg=60):
        df = self.bgstock.loadallstock()
        data = {}
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            name = bstockdic['name']
            arrys = self.getAmountInfo(code, rg)
            if arrys is None:
                continue
            arrys.append(name)
            data['row_' + str(i)] = arrys
        mdf  = pd.DataFrame.from_dict(data, orient='index', columns=['code', '流通市值', '总市值', '成交量基准', '启动成交量', '高位成交量', '名称'])
        mdf.to_excel(outfile + '.xlsx')
    @func_line_time    
    def winstock(self, code, date):
        backdentry = self.dystore.loadEntry(code, date, line=30)
        fwdentry = self.dystore.loadEntry(code, date, line=30, prv=False)
        backdentry['amchg'] = pollst.calpct(backdentry['amount'] , backdentry['amount'].shift(1))
        fwdentry['amchg'] = pollst.calpct(fwdentry['amount'] , fwdentry['amount'].shift(1))
        print(backdentry.max('amchg'))
    
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
        ffdt = newpd['trade_date'].to_numpy()
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
        dtlist = bchg['trade_date'].to_numpy()
        prvdate = None
        for dt in dtlist:
            prvfound = False
            ent = self.dystore.loadEntry(code, date=dt, line = 5)
            mfent = self.mfstore.loadEntry(code, date=dt, line = 5)
            dtl = ent['trade_date'].to_numpy()
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
        ffdt = newpd['trade_date'].to_numpy()
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
    def getMostAccDays(self, date):
        rdate = date
        if date == '':
            rdate = datetime.datetime.now().strftime('%Y%m%d')
        curst, curend = self.dystore.getStoreTMRange()
        if curend <= rdate:
            return curend
        if curst >= date:
            return curst
        dtflag = self.tradedate.isTrade(rdate) 
        if dtflag == 0:
            rdate = self.tradedate.getlasttrade(rdate)        
        return rdate

    def findsuit(self, date=''):
        rdate = self.getMostAccDays(date)
        df = self.bgstock.loadallstock()
        resdf = None
        vv=0
        for i in range(0, df.index.size):
            bstockdic = df.iloc[i].to_dict()
            code = bstockdic['symbol']
            self.calrate(code, rdate)
            self.actionp(code, rdate)
            #self.calup(code, rdate)

            #self.calHigh(code, rdate)    
    def loaddystorebyQuery(self, query):
        return self.dystore.mgquery(query)
    #@func_line_time
    def buildposts(self, code, date, node = 5, both = 0, abtest=1):
        df = self.dystore.getrecInfo(code, date, offset=0)
        if df is None:
            return None
        c = node
        basenode = pollst(code, date)
        basenode.buildfromdf(df)
        if abtest != 1:
            curnode = basenode
            while c > 0 and both <=0:
                newoffset = c - node - 1
                tmpdf = self.dystore.getrecInfo(code, date, offset=newoffset)
                if tmpdf is None:
                    break
                tmppost = pollst(code, tmpdf['trade_date'].to_numpy()[0])
                tmppost.buildfromdf(tmpdf)
                curnode.prev = tmppost
                tmppost.next = curnode
                curnode = tmppost
                c = c - 1
            c = node
            curnode = basenode
            while c > 0 and both >=0:
                newoffset = node - c + 1
                tmpdf = self.dystore.getrecInfo(code, date, offset=newoffset)
                if tmpdf is None:
                    break
                tmppost = pollst(code, tmpdf['trade_date'].to_numpy()[0])
                tmppost.buildfromdf(tmpdf)
                curnode.next = tmppost
                tmppost.prev = curnode
                curnode = tmppost
                c = c - 1
            return basenode
        if both <= 0:
            curnode = None
            backdf = self.dystore.loadEntry(code, date, node + 1)
            for i in range(0, len(backdf)):
                tmpsi = backdf.iloc[i]
                
                if tmpsi['trade_date'] == date:
                    if curnode is not None:
                        basenode.prev=curnode
                        curnode.next = basenode
                    break
                tmppost = pollst(code, tmpsi['trade_date'])
                tmppost.buildfromsi(tmpsi)
                if curnode is None:
                    curnode = tmppost
                else:
                    curnode.next = tmppost
                    tmppost.prev = curnode
                    curnode = tmppost
        if both >= 0:
            curnode = None
            fwddf = self.dystore.loadEntry(code, date, node + 1, prv=False)
            for i in range(0, len(fwddf)):
                tmpsi = fwddf.iloc[i]
                
                if tmpsi['trade_date'] == date:
                    assert(curnode is None)
                    curnode = basenode
                    continue
                tmppost = pollst(code, tmpsi['trade_date'])
                tmppost.buildfromsi(tmpsi)
                curnode.next = tmppost
                tmppost.prev = curnode
                curnode = tmppost
        return basenode
            


    '''
             or \
                (cpo.prev.pct_chg <= 0 and cpo.pct_chg >= 4 and pollst.calpct(cpo.prev.highp, cpo.openp) > 1):
    ''' 
    def adjustmonprice(self, code, mondate):
        retval = None
        cpo = self.buildposts(code, mondate, node=2)
        if cpo is None or cpo.prev is None or cpo.prev.prev is None:
            return retval
        cbchg = (cpo.pct_chg + cpo.prev.pct_chg)
        if cbchg>=15:
            if cpo.lowp > cpo.prev.closep:
                retval = round(cpo.closep * (100-cpo.pct_chg)/100, 2)
            else:
                retval = (cpo.prev.closep + cpo.lowp)/2

            if cpo.pct_chg>9.9:
                retval = round(cpo.closep * (100-cpo.pct_chg)/100, 2)

            if cpo.prev.prev.pct_chg > 9.9:
                retval = round((cpo.prev.prev.closep + cpo.prev.prev.openp)/2, 2)
            return retval

        if cbchg < 15 and cbchg >=11:
            retval = round(cpo.closep* (100 - cbchg/2)/100, 2)
            return retval

        if cpo.pct_chg > 0 and cpo.prev.pct_chg <= 4 and cbchg < 11:
            hgprice = max(cpo.prev.openp, cpo.prev.closep) 
            mnprice = min(cpo.prev.openp, cpo.prev.closep) 
            if (cpo.prev.pct_chg > 0 or (cpo.prev.pct_chg < 0 and cpo.pct_chg > 9)) and cbchg >= 3:
                retval = round((cpo.prev.highp + hgprice)/2, 2) 
            
            else:
                retval =  round((cpo.prev.lowp + mnprice)/2, 2) 
            if cpo.pct_chg > 5 and cpo.prev.pct_chg < -5:
                if min(cpo.openp, cpo.closep) > max(cpo.prev.openp, cpo.prev.closep):
                    retval = round(cpo.closep * (100 - cpo.pct_chg/2)/100)
            
            return retval 
        
        if cpo.pct_chg > 0 and cpo.prev.pct_chg > 4 and cbchg < 11:
            if cpo.prev.pct_chg < 9:
                hgprice = max(cpo.prev.prev.openp, cpo.prev.prev.closep)  
                retval = round(hgprice, 2) 
            else:
                retval = round(cpo.closep* (100 - cbchg/2)/100, 2)
            return retval 
        return retval 

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
    stoper.filldailydb()
    stoper.tradedate.initalldata()
    stoper.bgstock.savebgstockInfo()
    stoper.findsuit()
    rdate = '20200908'
    bgall = stoper.bgstock.loadallstock()
    for i in range(0, bgall.index.size):
        bstockdic = bgall.iloc[i].to_dict()
        code = bstockdic['symbol']
        name = bstockdic['name']
        rt = stoper.actionp(code, rdate) 
        if rt == 1:
            print('code: %s(%s), decs=%d'%(code, name, rt) )
    '''
    code='600824'
    code='000633'
    date='20200407'
    stoper.actionp(code, date)
    sys.exit(-1)
    rdf = stoper.getRealQuote(code)
    print(rdf)
    ent = stoper.dystore.loadEntry(code)
    nad = ent.get('amount').to_numpy()
    print(nad[0:2])
    #print(stoper.getAmountInfo(code))
    stoper.genamount(rg=120)
    '''
    #print(stoper.dystore.getrecInfo(code, '20200401',0))
    #print(stoper.actionp(code, '20200401'))
