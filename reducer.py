#!/usr/local/opt/python@3.7/bin/python3

import sys
import pymongo
from pymongo import MongoClient
import json
import ast
cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")

def jaccard_similarity(list1, list2): # Function for Jaccard Similarity
    s1 = set(list1) # List converted to Set
    s2 = set(list2) # List converted to Set
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))   # Intersection / Union is taken

json_sys = []
db  = cluster["dblp"]
coll = db["hadoops"]


for line in sys.stdin:  # mapper output is now taken from sys
    m = line.strip()
    a = eval(m)
    json_sys.append(a)

for i in range(len(json_sys)):
    x = coll.insert_one(json_sys[i])
    print(x)


# Copyrights @ 2021 , MusaDAC
# Last Update : April, 18 2021 4:18 am
