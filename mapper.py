#!/usr/local/opt/python@3.7/bin/python3

import sys
import pandas as pd
line_sys = []  # Array to store all splitted line coming from sys


for line in sys.stdin: # for loop to strip incoming line and then split and Ultimately storing in array to convert in pandas Ultimatly
    line = line.strip()
    word = line.split('\t')
    line_sys.append(word) # Appended processed String
    

df = pd.DataFrame(data = line_sys, columns = ['mdate', 'dblpkey', 'authors', 'title', 'year', 'journal']) # Processed Input file is converted to DataFrame
df = df.iloc[1:] # first row is dropped
df.sort_values(by=['authors']) # df data is sorted again on user just incase


temp = []

for i in df.authors.unique():
    df2 = df[df['authors'] == i]
    arr1 = df2.year
    arr2 = df2.title
    arr3 = df2.journal
    arr4 = df2.dblpkey
    farr1 = []
    farr2 = []
    farr3 = []
    farr4 = []
    for j in range (len(arr1)):
        farr1.append(arr1.iloc[j])
        farr2.append(arr2.iloc[j])
        farr3.append(arr3.iloc[j])
        try:
            farr4.append(arr4.iloc[j].split('/')[1])
        except:
            farr4.append('None')
    print({'author':i,'publication_year':farr1, 'publications':farr2, 'journals':farr3, 'journals_abbr':farr4 ,'start_career':max(farr1),'end_career':min(farr1)})

