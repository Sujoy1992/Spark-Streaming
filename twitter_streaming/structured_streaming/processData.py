import os
import time
import subprocess
import re
import csv
import sys

a1=sys.argv[1]
a2=sys.argv[2]
a3=sys.argv[3]


pattern1=r"^\d{2}\/\d{2}\/\d{4}$"
pattern2=r"^\d{2}\-\d{2}\-\d{4}$"

st = time.time()
csvfile1=a2

if int(a3) > 2015:
    index = 158
else:
    index = 148

with open(csvfile1, "w") as output:
    with open(a1) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if re.match(pattern1,row[index]):
                s=row[index]
                k=s.split('/')
                m=k[2]+"-"+k[0]+"-"+k[1]
                row[index]=m
                m=""

            if re.match(pattern2,row[index]):
                s=row[index]
                k=s.split('-')
                m=k[2]+"-"+k[1]+"-"+k[0]
                row[index]=m
                m=""

            for q in range(0,len(row)):
                row[q]=str(row[q])
                row[q]=row[q].replace(',',' ')

            writer=csv.writer(output, lineterminator='\n')
            writer.writerow(row)

print time.time() -st



