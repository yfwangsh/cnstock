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
import sys
sys.path.append(os.getcwd() + '/lib')

from lib.datastore import dailydataStore
from lib.datastore import stockInfoStore
from lib.datastore import moneyFlowdataStore
from lib.datastore import tickdataStore
from lib.stockops import tradeDate
from lib.stockops import storeoperator
if __name__ == "__main__":
    trd = tradeDate()
    trd.initalldata()

    stoper = storeoperator()
    stoper.filldailydb()
    stoper.debug = False
    stoper.findsuit()