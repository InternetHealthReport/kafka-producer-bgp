#for just route-views.wide

collectors = ["route-views.wide"]
timeStart = "2017-11-06T16:00:00"
timeEnd = "2017-11-06T22:00:00"

AF = 4
includedPeers = []
includedPrefix = []

from kafka import KafkaProducer
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from datetime import datetime
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks=0,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=65536,linger_ms=4000,compression_type='gzip')

def dt2ts(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())

def getBGPStream(recordType,AF,collectors,includedPeers,includedPrefix,startts,endts):
    stream = BGPStream()

    #recordType is supposed to be ribs or updates
    bgprFilter = "type " + recordType
    
    if AF == 6:
        bgprFilter += " and ipversion 6"
    else:
        bgprFilter +=  " and ipversion 4"

    for c in collectors:
        bgprFilter += " and collector %s " % c

    # if not self.asnFilter is None:
        # bgprFilter += ' and path %s$' % self.asnFilter
    for p in includedPeers:
        bgprFilter += " and peer %s " % p

    for p in includedPrefix:
        bgprFilter += " and prefix more %s " % p

    startts = datetime.strptime(startts+"UTC", "%Y-%m-%dT%H:%M:%S%Z")
    startts = dt2ts(startts)

    endts = datetime.strptime(endts+"UTC", "%Y-%m-%dT%H:%M:%S%Z")
    endts = dt2ts(endts)

    stream.parse_filter_string(bgprFilter)
    stream.add_interval_filter(startts, endts)

    return stream

def getRecordDict(record):
    recordDict = {}

    recordDict["project"] = record.project
    recordDict["collector"] = record.collector
    recordDict["type"] = record.type
    recordDict["dump_time"] = record.dump_time
    recordDict["time"] = record.time
    recordDict["status"] = record.status
    recordDict["dump_position"] = record.dump_position

    return recordDict

def getElementDict(element):
    elementDict = {}

    elementDict["type"] = element.type
    elementDict["time"] = element.time
    elementDict["peer_asn"] = element.peer_asn
    elementDict["peer_address"] = element.peer_address
    elementDict["fields"] = element.fields

    return elementDict

def pushRIBData(producer,AF,collector,includedPeers,includedPrefix,startts,endts):

    stream = getBGPStream("ribs",AF,[collector],includedPeers,includedPrefix,startts,endts)
    topicName = collector + "RIBHistoric"
    
    stream.start()

    rec = BGPRecord()

    while stream and stream.get_next_record(rec):
        completeRecord = {}
        completeRecord["rec"] = getRecordDict(rec)
        completeRecord["elements"] = []

        recordTimeStamp = rec.time

        recordTimeStamp = int(recordTimeStamp) * 1000

        elem = rec.get_next_elem()

        while(elem):
            elementDict = getElementDict(elem)
            completeRecord["elements"].append(elementDict)
            elem = rec.get_next_elem()

        # recordAsString = json.dumps(completeRecord)
        #print("Here is the record as a string: ",recordAsString)

        # recordAsBytes = bytes(recordAsString)

        #print("Topic: ",topicName," ,Time: ",recordTimeStamp)

        producer.send(topicName, completeRecord, timestamp_ms=recordTimeStamp)
        #producer.send(topicName,recordAsBytes)
        # print("Pushed a record to ",topicName," with timestamp ",recordTimeStamp)

    producer.flush()

def pushUpdateData(producer,AF,collector,includedPeers,includedPrefix,startts,endts):

    stream = getBGPStream("updates",AF,[collector],includedPeers,includedPrefix,startts,endts)
    topicName = collector + "UpdateHistoric"
    
    stream.start()

    rec = BGPRecord()

    while stream and stream.get_next_record(rec):
        completeRecord = {}
        completeRecord["rec"] = getRecordDict(rec)
        completeRecord["elements"] = []

        recordTimeStamp = rec.time

        recordTimeStamp = int(recordTimeStamp) * 1000

        elem = rec.get_next_elem()

        while(elem):
            elementDict = getElementDict(elem)
            completeRecord["elements"].append(elementDict)
            elem = rec.get_next_elem()

        # recordAsString = json.dumps(completeRecord)
        #print("Here is the record as a string: ",recordAsString)

        # recordAsBytes = bytes(recordAsString)

        #print("Topic: ",topicName," ,Time: ",recordTimeStamp)

        producer.send(topicName, completeRecord, timestamp_ms=recordTimeStamp)
        #producer.send(topicName,recordAsBytes)
        # print("Pushed a record to ",topicName," with timestamp ",recordTimeStamp)

    producer.flush()

for collector in collectors:
    print("A collector initiated")
    print("Downloading RIB data")
    pushRIBData(producer,AF,collector,includedPeers,includedPrefix,timeStart,timeEnd)
    print("Downloading UPDATE data")
    pushUpdateData(producer,AF,collector,includedPeers,includedPrefix,timeStart,timeEnd)


