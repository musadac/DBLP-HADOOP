
from lxml import etree
from lxml.etree import XMLSyntaxError
import pymongo
from pymongo import MongoClient



cluster  = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')


path_to_file = 'dblp-2021-04-01.xml'
dtd_path = 'dblp.dtd'

dtd = etree.DTD(dtd_path)

count = 0

db = cluster["dblp"]
coll = db['data']
context = etree.iterparse(path_to_file,dtd_validation=True, tag="phdthesis",events=('start', 'end'))
for event, element in context:
      dict = {}
      ee = []
      for child in element:
           if(child.tag == 'ee'):
                 ee.append(child.text)
           else:
                 dict[child.tag] = child.text
      dict['ee'] = ee
      coll.insert_one(dict)
      element.clear()
      del dict
      del ee
      count = count + 1
      print(event ,count)
