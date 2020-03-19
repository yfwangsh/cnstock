from pymongo import MongoClient
import pandas as pd
import numpy
import datetime
import unittest
import os
import copy
import json
import time
from sklearn.svm import SVC
import sys,getopt
import traceback

from concurrent.futures import ThreadPoolExecutor,as_completed

sys.path.append(os.getcwd() + '/lib')

from lib.datastore import dailydataStore
from lib.datastore import stockInfoStore
from lib.datastore import moneyFlowdataStore
from lib.datastore import tickdataStore
from lib.stockops import tradeDate
from lib.stockops import storeoperator
from lib.stockops import pollst
from lib.stockops import DingTalk

running=False
monflag=False
def getTaskParamList(trdstore, count):
   retarray = []
   datenow = datetime.datetime.now().strftime('%Y%m%d')
   trade_date = trdstore.getlasttrade(datenow)
   if trade_date is None:
      trdstore.initalldata()
      trade_date = trdstore.getlasttrade(datenow)        
   while len(retarray) < count:
       if trdstore.isTrade(trade_date):
           retarray.append(trade_date)
       trade_date = trdstore.getlasttrade(trade_date)
   return retarray

def Storedailyinit(stoper, tcount):
   datecount = tcount
   stoper.dtalk.send_msg('Storedailyinit  job start!')
   processmsg = ''
   try:
      stoper.tradedate.initalldata()
      stoper.filldailydb()
      processmsg = 'Storedailyinit done with filldailydb'
      dtparam = getTaskParamList(stoper.tradedate, datecount)
      for dt in dtparam:
         processmsg = 'Storedailyinit start findsuit:' + dt
         stoper.findsuit(dt)
         processmsg = 'Storedailyinit done with findsuit:' + dt
      processmsg = 'Storedailyinit done with findsuit'
      stoper.updateMonitorSet()
      processmsg = 'Storedailyinit done with update MonitorSet'
   except Exception as identifier:
      stoper.dtalk.send_msg('Storedailyinit job run with exception[' + str(identifier) + '] at (' + processmsg + ')')
   stoper.dtalk.send_msg('Storedailyinit job end!')
def timeforterm():
   nowtime = datetime.datetime.now()
   amtmstart =datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 9,0,0,0)
   pmtmend = datetime.datetime(nowtime.year, nowtime.month, nowtime.day, 15,00,0,0)
   if nowtime <  amtmstart or  nowtime > pmtmend:
       return True
   return False
def getstockentry(stoper, code ,date):
   query = {'trade_date': {'$gte': date}, 'ts_code': stockInfoStore.canoncode(code)}
   return stoper.dystore.mgquery(query)
def rtmonitorwork(stoper, taskname):
   global running
   global monflag
   codecache = {}
   warncache={}
   pctcache={}
   bidcache={}
   threaddtalk = DingTalk()
   threaddtalk.setURL('https://oapi.dingtalk.com/robot/send?access_token=' + stoper.conf.getConf('dtalk', 'inv_access_token'))
   threaddtalk.initSec(stoper.conf.getConf('dtalk','inv_secret'))
   threaddtalk.send_msg('日常投资监控正常运行!')
   while running:
      df = stoper.getCMonitor()
      if df is not None and not df.empty and monflag:
         for rr in df.itertuples(index=False):
            msg=''
            try:               
               mydict = rr._asdict()
               code = mydict['code']
               price = mydict['Pricetarget']
               rq = storeoperator.getRealQuote(code)
               realprice = float(rq['price'].to_numpy()[0])
               name = rq['name'].to_numpy()[0]
               preclose = float(rq['pre_close'].to_numpy()[0])
               ptime = rq['time'].to_numpy()[0]
               issend = False
               if realprice==0:
                  realprice = float(rq['bid'].to_numpy()[0])
                  pct = pollst.calpct(realprice, preclose)
                  msg =  name+ '(' + code  +  ')[' + str(realprice) +'  <' + str(round(pct,2)) + '%>]'
                  if pct > 5 and \
                     (not bidcache.__contains__(code) or \
                        (bidcache[code] & 2 )== 0 ):
                           if bidcache.__contains__(code):
                              bidcache[code] = bidcache[code]|2
                           else:
                              bidcache[code] = 2
                           msg = msg + ' $高位竞价! '
                           issend = True                  
                  if pct < -5 and \
                     (not bidcache.__contains__(code) or \
                        (bidcache[code] & 4 )== 0 ):
                           if bidcache.__contains__(code):
                              bidcache[code] = bidcache[code]|4
                           else:
                              bidcache[code] = 4
                           msg = msg + ' $低位竞价! '
                           issend = True                  
               else:
                  pct = pollst.calpct(realprice, preclose)
                  #print('code=[' + str(code) + ']price:' + str(realprice)+ ', pct=' +str(pct) + ',pclose=' + str(preclose))
                  msg =  name+ '(' + code  +  ')[' + str(realprice) +' < ' + str(round(pct,2)) + '%>]'
                  if codecache.__contains__(code):
                     bsptstr = codecache[code][0]
                     bsp = codecache[code][1]
                     lstkp = codecache[code][2]
                     bsptm = datetime.datetime.strptime(bsptstr,'%H:%M:%S')
                     ptm = datetime.datetime.strptime(ptime,'%H:%M:%S')
                     codecache[code] = (bsptstr, bsp, realprice)

                     if (ptm - bsptm) >= datetime.timedelta(seconds = 45):
                        codecache[code] = (ptime, realprice, realprice)
                        if pollst.calpct(realprice, bsp) <= -0.8:
                           issend = True
                           msg = msg + '$迅速回落!' 
                        if pollst.calpct(realprice, bsp) >= 0.8:
                           issend = True
                           msg = msg + ' $火箭发射!'
                     else:
                        if pollst.calpct(realprice, lstkp) <= -0.4:
                           codecache[code] = (ptime, realprice, realprice)
                           issend = True
                           msg = msg + '$迅速回落!' 
                        if pollst.calpct(realprice, lstkp) >= 0.4:
                           codecache[code] = (ptime, realprice, realprice)
                           issend = True
                           msg = msg + ' $火箭发射!'
                  else:
                     codecache[code] = (ptime, realprice, realprice)

                  if realprice > price:
                     if warncache.__contains__(code):
                        if pollst.calpct(realprice, warncache[code]) >= 0.5:
                           warncache[code] = realprice
                           issend = True
                           msg = msg + '$持续超预警价格!'
                     else:
                        warncache[code] = realprice
                        issend = True
                        msg = msg + '$超出预警价格!'
                  elif realprice < price * 0.9:
                     if warncache.__contains__(code):
                        if pollst.calpct(realprice, warncache[code]) <= -0.5:
                           warncache[code] = realprice
                           issend = True
                           msg = msg + '$持续低于预警价格90%!'
                     else:
                        warncache[code] = realprice
                        issend = True
                        msg = msg + '$低于预警价格90%!'

                  if pct > 3:
                     if not pctcache.__contains__(code):
                        pctcache[code] = pct
                        msg = msg + '$今日涨幅大！'
                        issend = True
                     else:
                        if (pct - pctcache[code]) > 1:
                           msg = msg + '$今日持续涨！' 
                           issend = True
                           pctcache[code] = pct
               msg = msg + ' send@' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
               if issend:
                  threaddtalk.send_msg(msg)
                  print(msg)
            except Exception as err:
               print('thread-' + taskname + ':' + str(err))
         time.sleep(5)
   retmsg='thread for ' + taskname + ' stopped'
   return retmsg 

def checkwork(stoper, date):
   global running
   global monflag
   df = stoper.getMonitorDaySet(date)
   if df is None or df.empty:
      retmsg='thread for ' + date + ' has nothing to process, stopped!'
      return retmsg
   warningset = {}
   skipcodecache = {}
   skipcodecache['init'] = False
   while running:
      if monflag:
         for rr in df.itertuples(index=False):
            try:
               mydict = rr._asdict()
               code = mydict['code']
               date = mydict['trade_date']
               price = mydict['Pricetarget']
               if not skipcodecache['init']:
                  cdf = getstockentry(stoper, code,date)
                  if cdf is not None and not cdf.empty and numpy.min(cdf['low'])<=price:
                     skipcodecache[code] = date
                     continue
               else:
                  if skipcodecache.__contains__(code):
                     continue
               realprice = float(storeoperator.getRealQuote(code)['price'].to_numpy()[0])
               name = storeoperator.getRealQuote(code)['name'].to_numpy()[0]
               if realprice==0:
                  realprice = float(storeoperator.getRealQuote(code)['bid'].to_numpy()[0])
               pct = abs(round((price-realprice)/realprice * 100,2 ))
               if pct <= 1.5 :
                  if warningset.__contains__(code):
                     oldprice = warningset[code]
                     if round((oldprice - realprice)/price *100, 2) < 0.3:
                        continue
                  warningset[code]=realprice
                  msg =  name + ',[' + code +  '] current:' + str(realprice) + ' target:' + str(price)
                  msg = msg + ' monitor begin@' + date
                  msg = msg + ' send@' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                  stoper.dtalk.send_msg(msg)
                  print(msg)
            except Exception as err:
               print('thread-' + date + ':' + str(err))
               #traceback.print_exc() 
         skipcodecache['init'] = True
         time.sleep(5)
   retmsg='thread for ' + date + ' stopped'
   return retmsg 

def MonitorThread(stoper, tcount):
   datenow = datetime.datetime.now().strftime('%Y%m%d')
   if  not ForceMonitorRun() and not stoper.tradedate.isTrade(datenow) :
      print('out of working time')
      return 
   threadcount = tcount + 1
   executor = ThreadPoolExecutor(threadcount )
   stoper.dtalk.send_msg('Monitor Thread starting')   
   try:
      global running
      global monflag
      trd = stoper.tradedate      
      taskparams = getTaskParamList(trd, tcount)
      running=stoper.getThreadRunningFlag()
      taskname = 'futureMonitor'
      all_task=[]
      for para in taskparams:
         future = executor.submit(checkwork, stoper, para)
         all_task.append(future)
      fut = executor.submit(rtmonitorwork, stoper, taskname)
      all_task.append(fut)
      while running:
         time.sleep(5)
         #monflag = True
         #running = stoper.getThreadRunningFlag()
         monflag = stoper.tradedate.isTradingNow()
         running = stoper.getThreadRunningFlag() and not timeforterm()
      for fu in as_completed(all_task):
         data = fu.result()
         print(f"main: {data}")
   except Exception as identifier:
      print('mainthread exception:' + str(identifier))
      traceback.print_exc() 
   finally:
      executor.shutdown()
      stoper.dtalk.send_msg('Monitor Thread shut down')
   
def ForceMonitorRun():
   return False # debug Turn to True
def main(argv):
   stoper = storeoperator()
   stoper.tradedate.initalldata()
   stoper.debug = False
   stoper.dtalk.setURL('https://oapi.dingtalk.com/robot/send?access_token=' + stoper.conf.getConf('dtalk', 'alg_access_token'))
   func = ''
   threadc = 5
   try:
      opts, args = getopt.getopt(argv,"he:c:",["exec=","threadcount="])
   except getopt.GetoptError:
      print('run.py -e <func> -c <number>')
      sys.exit(2)
   
   for opt, arg in opts:
      if opt == '-h':
         print('run.py -e <func> -c <number>')
         sys.exit()
      elif opt in ("-e", "--exec"):
         func = arg
      elif opt in ("-c", "--threadcount"):
         threadc = int(arg)
         if threadc < 0:
            threadc = 5
   if func == 'dinit':
      Storedailyinit(stoper, threadc)
   elif func == 'monitor':
      MonitorThread(stoper, threadc)
   else:
      print('execution not support!')
     
if __name__ == "__main__":
   main(sys.argv[1:])
   

