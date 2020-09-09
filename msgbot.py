import sys
import os
import getopt

import http.client
from urllib.parse import urlparse
import time
import requests
sys.path.append(os.getcwd() + '/lib')
sys.path.append(os.getcwd())
from lib.stockops import DingTalk
from utils import MyConf


def main(argv):
    msg = None
    try:
        opts, args = getopt.getopt(argv,"hm:",['msg='])
    except getopt.GetoptError:
        print('msgbot.py -m <msg>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('tickclaw.py -m <mode>')
            sys.exit(0)
        elif opt in ("-m", "--msg"):
            msg = arg
    if msg is None:
        print('message is required, usage: msgbot.py -m <mode>')
        sys.exit(-1)
    dtalk = DingTalk()
    conf = MyConf('conf/mystock.conf')
    dtalk.setURL('https://oapi.dingtalk.com/robot/send?access_token=' + conf.getConf('dtalk', 'alg_access_token'))
    dtalk.send_msg(msg)            
if __name__ == '__main__':
    main(sys.argv[1:])
    
