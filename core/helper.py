import urllib
import json
import re
import os
import requests
from pprint import pprint
import xmltodict
import logging

from pymongo import MongoClient
import json
from datasketch import MinHash, MinHashLSH
import math

FILE_URL = "./core/config.xml"


def getConfig():
    with open(FILE_URL, "r") as f:
        lines = f.readlines()
    temp = ''
    for line in lines:
        temp += line
    config = xmltodict.parse(temp)
    return config


def read_database_config():
    db_conf = {}

    config = getConfig()

    db_conf["mongodb_host"] = config['mongodb']['host']
    db_conf["mongodb_port"] = config['mongodb']['port']
    db_conf["mongodb_password"] = config['mongodb']['password']
    db_conf["mongodb_username"] = config['mongodb']['username']

    return db_conf

def nCr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

def nPr(n,r):
    f = math.factorial
    return f(n) / f(n-r)

def querry_lsh(querry_attribute, set_attribute, attribute_name, threshold=0.5 ):

    attr_num = len(set_attribute)

    # Create LSH index
    lsh = MinHashLSH(threshold=0.5, num_perm=128)

    for i in range(0, attr_num):
        #Create MinHash
        m = MinHash(num_perm=128)

        temp_str = set(set_attribute[i])
        #Caculate min hash
        for d in temp_str:
            m.update(d.encode('utf8'))
        #insert MinHash in LSH index
        lsh.insert(attribute_name['i'], m)

        #Delete m for safety
        del m

    m_querry = MinHash(num_perm=128)
    for d in querry_attribute:
        m_querry.update(d.encode('utf8'))

    result = lsh.query(m_querry)
    return result