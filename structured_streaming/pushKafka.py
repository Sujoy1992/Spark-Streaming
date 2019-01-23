import sys
import csv
import time
import json
from kafka import KafkaProducer
from collections import OrderedDict
import datetime


fileToBeRead = sys.argv[1]
rSchema = sys.argv[2]


class streamKafka:


    def __init__(self):
        self.producer = None

        try:
            self.producer = KafkaProducer(bootstrap_servers=['10.81.1.158:9092'], api_version=(0, 10))
        except Exception as ex:
            print ex
        
        with open(rSchema) as f:
            self.schema = json.load(f)


    def createJson(self,value):
        tempDict = OrderedDict()
        f_value = value.split(',')
        
        for (e,(key,val)) in enumerate(zip(self.schema,f_value)):
            if e == 157:
                val = float(val)
            tempDict[key] = val

        schemaJson = '%s' % (json.dumps(tempDict))

        return schemaJson

    def publish_message(self,topic_name,value):
        row = ''.join(value)
        

        producer = self.producer.send(topic_name,row)

        while not producer.is_done:
            continue

    def readFile(self):
        with open(fileToBeRead) as f:
            for row in f.readlines():
                row = row.strip('\n')
                kv = self.createJson(row)
                print kv
                self.publish_message('new3', kv)
                break

def main():
    streamKafkaObj = streamKafka()
    streamKafkaObj.readFile()



if __name__ == '__main__':
    main()
