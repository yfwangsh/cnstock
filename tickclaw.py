# encoding:utf-8
# An IIS short_name scanner    


import sys
import getopt
from pymongo import MongoClient
import copy
import http.client
from urllib.parse import urlparse
import threading
import queue
import time
import requests
import pandas as pd
import numpy
import datetime
from bs4 import BeautifulSoup
import traceback
import threading
import os
import re
import json
from requests.exceptions import ProxyError
from requests.exceptions import ConnectTimeout
from requests.exceptions import ReadTimeout
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError
sys.path.append(os.getcwd() + '/lib')
sys.path.append(os.getcwd())
from lib.stockops import DingTalk
from lib.datastore import dbbase
from lib.datastore import stockInfoStore
from utils import MyConf
proxypool_url = 'http://114.67.90.19:8055/random'
target_url = 'http://httpbin.org/get'
localproxies = {'http': 'http://127.0.0.1:9090'}
my_headers = {
        'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12H143',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding' : 'gzip',
        'Accept-Language' : 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4'
        }
        
def get_random_proxy():
    """
    get random proxy from proxypool
    :return: proxy
    """
    return requests.get(proxypool_url, proxies=localproxies).text.strip()
class crawdb(dbbase):
    def __init__(self, mgclient=MongoClient('mongodb://localhost:27017/')['infofina']):
        super(crawdb, self).__init__(mgclient)
        self.collection='news'
        self.keydictarray=['sid']

    def addinfo(self, msgdic):
        msgset = self.mongoClient[self.collection]
        keydic={}
        cpdict = copy.deepcopy(msgdic)
        for dickey in self.keydictarray:
            keydic[dickey] = msgdic[dickey]
            del cpdict[dickey]
        msgset.update_one(keydic,{'$setOnInsert': {'upflag':1}, '$set':cpdict}, upsert=True)

class TickerScanner():
    def __init__(self):
        self.alphanum = 'abcdef0123456789'
        self.files = []
        self.dirs = []
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.threads = []
        self.request_method = 'OPTIONS'
        self.msg_queue = queue.Queue()
        self.STOP_ME = False
        #threading.Thread(target=self._print).start()
    def crawticks(self, code, date):
        ccode = stockInfoStore.snfmcode(code)
        ipg = 1
        targetUrl = 'http://market.finance.sina.com.cn/transHis.php?symbol=' + ccode + '&date=' + date + '&page='
        fdn_list = []
        runp = None
        runpc = 0
        while True:
            try:
                if runpc > 10:
                    runpc = 0
                if runpc == 0:
                    proxy = get_random_proxy()
                    proxies = {'http': 'http://' + proxy}
                    runp = proxies 
                fturl = targetUrl + str(ipg)
                sess = requests.Session()
                r = sess.get(fturl, proxies= runp, headers = my_headers, timeout=(8, 13))  
                if r.status_code != 200:
                    runpc = 0
                    continue
                r.encoding = 'gb2312'
                soup = BeautifulSoup(r.text, 'lxml') 
                tables = soup.select('table')
                df_list = []
                for table in tables:
                    df_list.append(pd.concat(pd.read_html(table.prettify())))
                if len(df_list) == 0:
                    print(r.text)
                    lk = soup.select_one('link')
                    if lk is not None and lk.attrs['href'] is not None and lk.attr['href'].startswith('http://www.sinaimg.cn'):
                        break
                    else:
                        runpc = 0
                        continue
                print('use proxy: ' + runp.get('http'))
                runpc += 1
                df = pd.concat(df_list)
                newdf = df.rename(index=str,columns={"成交时间":"time", "成交价":"price", "价格变动":"change", "成交量(手)": "volume", "成交额(元)":"amount","性质":"type"})
                if newdf.empty:
                    break
                fdn_list.append(newdf.replace('--', 0))                
                ipg = ipg + 1
            except ReadTimeout:
                print(' proxy read timeout continue ')
                continue
            except ConnectTimeout:
                runpc = 0
                print(' proxy connect timeout continue ')
                continue
            except ProxyError:
                runpc = 0
                print(' proxy error continue ')
                continue
            except ChunkedEncodingError:
                runpc = 0
                print('proxy connect reset')
                continue
            except ConnectionError:
                runpc = 0
                print('proxy connection fail')
                continue
            except Exception as ex:
                runpc = 0
                print(type(ex))
                print('exception:' + str(ex))
                traceback.print_exc() 
                break
        if len(fdn_list) == 0:
            return None
        fndf = pd.concat(fdn_list)
        fndf.reset_index(drop=True, inplace=True)
        return fndf
class infowatcher:
    def __init__(self):
        self.conf = MyConf('conf/mystock.conf')
        self.watchurl = 'http://zhibo.sina.com.cn/api/zhibo/feed?callback=runq&page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=20&'
        self.headers = {'Accept':  '*/*',
               'Accept-Encoding':'gzip, deflate',
               'Accept-Language':'zh-CN, zh; q=0.9',
               'Pragma':'no-cache',
               'Cache-Control':'no-cache',
               'Connection':'keep-alive',
               'Upgrade-Insecure-Requests': '1',
               'Referer':'http://finance.sina.com.cn/7x24/',
               'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'}
        self.matchpattern = re.compile(r'try\{runq\((.*)\);\}catch\(e\)\{\};')
        self.dtalk = DingTalk()
        self.dtalk.setURL('https://oapi.dingtalk.com/robot/send?access_token=' + self.conf.getConf('dtalk', 'alg_access_token'))
        self.initRun()
        self._STOP = False
        self.lastid = None
        self.crawdb = crawdb()

        threading.Thread(target=self._threadstop).start()
    def initRun(self):
        self.debug = os.path.exists('/tmp/gwdbg')
        if os.path.exists('infolock'):
            os.remove('infolock')

    def run(self):
        try:
            threading.Thread(target=self._threadrun).start()
        except KeyboardInterrupt:
            sys.exit(0)
    def _threadstop(self):
        while not self._STOP:
            if os.path.exists('infolock'):
                self._STOP = True
            time.sleep(10)
            self.debug = os.path.exists('/tmp/gwdbg')
    def _threadrun(self):
        sess = requests.Session()
        count = 0
        while not self._STOP:
            count+=1
            try:
                ts = '%d'%(int(round(time.time() * 1000)))
                result =  self.doquery(sess,ts)
                if result is not None:
                    retarray = self.parsetodb(result) 
                    for i in range(0, retarray.__len__())[::-1]:
                        sid = retarray[i].get('id')
                        if self.lastid is None or (self.lastid is not None and sid > self.lastid):
                            self.lastid = sid
                            msgdic = {}                            
                            msg ='[财经新闻] %s: %s'%(retarray[i].get('update_time'), retarray[i].get('rich_text'))
                            print(msg)
                            msgdic['sid'] = sid
                            msgdic['update_time'] = retarray[i].get('update_time')
                            msgdic['rich_text'] = retarray[i].get('rich_text')
                            self.crawdb.addinfo(msgdic)
                            #self.dtalk.send_msg(msg) 
            except KeyboardInterrupt:
                return               
            except Exception as ex:
                traceback.print_exc()
                print(type(ex))
            time.sleep(55)
    def parsetodb(self, intext):
        retarry = []
        match = re.match(self.matchpattern, intext)
        if match:
            for item in match.groups():
                jsonobj = json.loads(item)
                if jsonobj.get('result') is not None and \
                    jsonobj.get('result').get('data') is not None and \
                    jsonobj.get('result').get('data').get('top') is not None and \
                    jsonobj.get('result').get('data').get('top').get('list') is not None:
                    retarry = jsonobj.get('result').get('data').get('feed').get('list')
        else:
            print('not match')
        return retarry
    def doquery(self, sess,  ts):
        try:
            url = 'http://zhibo.sina.com.cn/api/zhibo/feed?callback=runq&page=1&page_size=5&zhibo_id=152&tag_id=0&dire=f&dpc=1&pagesize=5&type=0&_=' + ts
            if self.lastid is not None:
                url = self.watchurl + 'id=%s&type=0&_=%s'%(self.lastid, ts) 
            r = sess.get(url, headers=self.headers)
            if r.status_code == 200:
                return r.text
            return None
            #timeout=(8, 13)
        except Exception as ex:
            traceback.print_exc()
            print(type(ex))
    
class goldwatcher:
    def __init__(self):
        self.conf = MyConf('conf/mystock.conf')
        self.watchurl = 'https://hq.sinajs.cn/?_='
        self.watchpara = '&list=hf_GC'
        self.headers = {'Accept':  '*/*',
               'Accept-Encoding':'gzip, deflate, br',
               'Accept-Language':'zh-CN, zh; q=0.9',
               'Pragma':'no-cache',
               'Cache-Control':'no-cache',
               'Connection':'keep-alive',
               'Sec-Fetch-Dest':'script',
               'Sec-Fetch-Mode':'no-cors',
               'Sec-Fetch-Site':'cross-site',
               'Referer':'https://finance.sina.com.cn/futures/quotes/GC.shtml',
               'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'}
        self.matchpattern = re.compile(r'var\s[\w]+="([\S]+)";')
        self.dtalk = DingTalk()
        self.dtalk.setURL('https://oapi.dingtalk.com/robot/send?access_token=' + self.conf.getConf('dtalk', 'alg_access_token'))
        self.initRun()
        self._STOP = False
        threading.Thread(target=self._threadstop).start()
    def initRun(self):
        self.debug = os.path.exists('/tmp/gwdbg')
        if os.path.exists('locktk'):
            os.remove('locktk')

    def run(self):
        try:
            threading.Thread(target=self._threadrun).start()
        except KeyboardInterrupt:
            sys.exit(0)
    def _threadstop(self):
        while not self._STOP:
            if os.path.exists('locktk'):
                self._STOP = True
            time.sleep(10)
            self.debug = os.path.exists('/tmp/gwdbg')
    def _threadrun(self):
        sess = requests.Session()
        count = 0
        basedic = None
        while not self._STOP:
            count+=1
            try:
                ts = '%d'%(int(round(time.time() * 1000)))
                result =  self.doquery(sess,ts)
                if result is not None:
                    cdict = self.parsetodb(result)
                    if self.debug:
                        print(cdict)
                    if basedic is None:
                        basedic = cdict
                        warsig = (float(cdict['open']) - float(cdict['prev']))*100/ float(cdict['prev'])
                        if warsig > 1:
                            self.dtalk.send_msg('%s@%s启动盯盘,  开盘价%s，昨收盘%s, 涨幅%f, 当前价%s'%(cdict['name'], cdict['date'] + ' ' + cdict['time'], cdict['open'],cdict['prev'],warsig, cdict['price'])) 
                    else:
                        tmstr = basedic['date'] + ' ' + basedic['time']
                        lastm = time.strptime(tmstr, '%Y-%m-%d %H:%M:%S')
                        ctime = time.strptime(cdict['date'] + ' ' + cdict['time'], '%Y-%m-%d %H:%M:%S')
                        gapsec = time.mktime(ctime) - time.mktime(lastm)
                        prg = (float(cdict['price']) - float(basedic['price']))*100/ float(basedic['price'])
                        if gapsec == 0.00 and cdict['price'] == basedic['price'] and count > 5:
                            self._STOP = True
                            msg = '%s@%s收盘,当前收盘价%s, 开盘价%s'%(cdict['name'], cdict['date'] + ' ' + cdict['time'], cdict['price'], cdict['open'])
                            self.dtalk.send_msg(msg)  
                            continue                      
                        if prg > 1 or prg < -1:
                            basedic = cdict
                            tm = basedic['date'] + ' ' + basedic['time']
                            pct = (float(basedic['price']) - float(basedic['open']))*100/ float(basedic['open'])
                            msg ='%s@%s,在%d秒时间内，变化幅度%f,当前价%s, 开盘价%s, 涨幅:%f'%(basedic['name'], tm, gapsec, prg, basedic['price'], basedic['open'], pct) 
                            self.dtalk.send_msg(msg)                    
                        elif prg < 0:
                            basedic = cdict
            except KeyboardInterrupt:
                return               
            except Exception as ex:
                traceback.print_exc()
                print(type(ex))
            time.sleep(3)
    def parsetodb(self, intext):
        retdic = {}
        match = re.match(self.matchpattern, intext)
        if match:
            for item in match.groups():
                arry = item.split(',')
                retdic['price'] = arry[0]
                retdic['buy'] = arry[2]
                retdic['sell'] = arry[3]
                retdic['high'] = arry[4]
                retdic['low'] = arry[5]
                retdic['time'] = arry[6]
                retdic['prev'] = arry[7]
                retdic['open'] = arry[8]
                retdic['holdamount'] = arry[9]
                retdic['a1'] = arry[10]
                retdic['a2'] = arry[11]
                retdic['date'] = arry[12]
                retdic['name'] = arry[13]
                retdic['amount'] = arry[14]
        else:
            print('not match')
        return retdic
    def doquery(self, sess,  ts):
        try:
            url = self.watchurl + ts +self.watchpara
            r = sess.get(url, headers=self.headers)
            if r.status_code == 200:
                return r.text
            return None
            #timeout=(8, 13)
        except Exception as ex:
            traceback.print_exc()
            print(type(ex))
def runinfowatch():
    iwf = infowatcher()
    iwf.run()
def rungoldwatch():
    gd = goldwatcher()
    gd.run()
def main(argv):
    mode = None
    try:
        opts, args = getopt.getopt(argv,"hm:",['mode='])
    except getopt.GetoptError:
        print('tickclaw.py -m <mode>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('tickclaw.py -m <mode>')
            sys.exit(0)
        elif opt in ("-m", "--mode"):
            mode = arg
    if mode is None:
        print('mode is required, usage: tickclaw.py -m <mode>')
        sys.exit(-1)
    if mode == 'gd':
        rungoldwatch()
    elif mode == 'info':
        runinfowatch()
        
if __name__ == '__main__':
    main(sys.argv[1:])
