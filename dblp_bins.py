import pandas as pd
import numpy as np
from statistics import mode
import pymongo
from pymongo import MongoClient

cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")

db  = cluster["dblp"]
coll = db["hadoops-inproceedings"]

db2  = cluster["dblp"]
coll2 = db2["bins"]

def sorted_mapped(tup):
    years=tup[0]
    years=[float(x)for x in years]
    publications=tup[1]
    conferences=tup[2]
    field=tup[3]
    zipper=zip(years,publications,conferences,field)
    lis=sorted(zipper)
    return lis


def mapper(dataset):
    new_dataset=[]
    current_year=None
    arr=[]
    for x in dataset:
        if current_year is None:
            current_year=x[0]
            arr.append([current_year])
            arr.append([x[1]])
            arr.append([x[2]])
            arr.append([x[3]])
        else:
            if x[0] == current_year:
                arr[1].append(x[1])
                arr[2].append(x[2])
                arr[3].append(x[3])

            elif x[0]!=current_year:
                new_dataset.append(arr)
                arr=[]
                current_year=x[0]
                arr.append([current_year])
                arr.append([x[1]])
                arr.append([x[2]])
                arr.append([x[3]])

    new_dataset.append(arr)
    return new_dataset



def mapping_with_range_of_years(new_dataset):
    arr=[[],[],[],[]]
    don_check=[]
    ne_dataset=[]
    for i in range(0,len(new_dataset)):
        if i in don_check:
            continue
        item=new_dataset[i]
        year=float(item[0][0])
        item[0].append(year+4)
        arr[0].append(item[0])
        arr[1].extend(item[1])
        arr[2].extend(item[2])
        arr[3].extend(item[3])
    
        for x in range(i+1,i+5):
            if x>=len(new_dataset):
                break
#             print(x,len(new_dataset))
            new_item=new_dataset[x]
            year1=float(new_item[0][0])
            
            if year1<=year+4 and year<=year1:
#                 print(year1,year)

                arr[1].extend(new_item[1])
                arr[2].extend(new_item[2])
                arr[3].extend(new_item[3])
                don_check.append(x)
                
        ne_dataset.append(arr)
        arr=[[],[],[],[]]
#     print(ne_dataset)
    return ne_dataset
    

def mapping(row):
    row=list(row)
    row=sorted_mapped(row)
    row=mapper(row)
    row=mapping_with_range_of_years(row)
    return row
    

leo = coll.find({})
db=None
count=0
for i in leo:
    author = i['author']
    year = i['publication_year']
    publication = i['publications']
    conf = i['conf_abbr']
    ra = i['research area']
    start_career = i['end_career']
            
    ddd = {
        "year":[year],
        "publication":[publication],
        "conf":[conf],
        "fields":[ra]
    }
            
    
    db = pd.DataFrame(ddd)
    count=count+1
    
    
    x=db.apply(mapping,axis=1)
    bins = []
    for i in range(len(x[0])):
        a = {
           "year":x[0][i][0],
           "title": x[0][i][1],
           "conf":x[0][i][2],
           "subjects":x[0][i][3]
        }
        bins.append(a)
    fi = {
        "author":author,
        "bins":bins
    }
    coll2.insert_one(fi)
