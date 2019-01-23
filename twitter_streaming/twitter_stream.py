import time
import sys
import json
import ConfigParser
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from collections import OrderedDict

#commandLine args
brokers = sys.argv[1]
topic = sys.argv[2]
config_file = sys.argv[3]
schema = ["created_at","text","screen_name","location","friends_count"]

class Listener(StreamListener):

    def __init__(self):
        self.producer = None

        try:
            self.producer = KafkaProducer(bootstrap_servers=[brokers])
        except Exception as ex:
            print ex
    
    def on_data(self, data):
        tempDict = OrderedDict()
        tempList = list()

        def modifyTimestamp(data):
            finalTimestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data,'%a %b %d %H:%M:%S +0000 %Y'))
            return finalTimestamp


        def pushToKafka(temp_dict):
            dataToBePushed = (json.dumps(temp_dict))
            producer = self.producer.send(topic, dataToBePushed)

            while not producer.is_done:
                continue
        
        loadData = json.loads(data)
        if len(loadData.keys()) > 2:
            timestamp = modifyTimestamp(loadData["created_at"])
            tempList.append(timestamp)
            tempList.append(loadData["text"])
            tempList.append(loadData["user"]["screen_name"])
            tempList.append(loadData["user"]["location"])
            tempList.append(loadData["user"]["friends_count"])

            for key,val in zip(schema,tempList):
                tempDict[key] = val
        
        print tempDict
        print '-----------------------------------------------------------------------------------------------------------------'

        pushToKafka(tempDict)
        return True


    def on_error(self, status):
        print status


def main():

    config = ConfigParser.RawConfigParser()
    config.read(config_file) 

    hashTagList = config.get('config','HASHTAG_LIST') 
    CONSUMER_KEY = config.get('config','CONSUMER_KEY')
    CONSUMER_SECRET = config.get('config','CONSUMER_SECRET')
    ACCESS_TOKEN = config.get('config','ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = config.get('config','ACCESS_TOKEN_SECRET')

    listener = Listener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)
    stream.filter(locations=[-122.75,36.8,-121.75,37.8,-74,40,-73,41])



if __name__ == '__main__':
    main()


