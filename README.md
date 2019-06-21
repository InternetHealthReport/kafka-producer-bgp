# kafka-producer-bgp
This script pushes BGPStream data against specified collector(s) for the specified time window to Kafka topic(s).

### Example
You must specify collector name(s) (**-c**), record type (**-t**), a start time (**-s**) and an end time (**-e**).
The format for time is **Y-m-dTH:M:S**
Record type can be RIB or Update
```
$ python simpleProducer.py -c rrc19 -t RIB -s 2017-11-06T16:00:00 -e 2017-11-06T22:00:00
```

### Topic Name
Topics are separated by collector name, record type (RIB or Update), and whether the data is live (read from RIS Live) or historic (BGPStream data). Each topic name is given by (ihr) + (collectorName) + (rib or update). For example, the separate topic for **RIB** data for **rrc10** is ihr__rrc10_rib**.

### Record Structure
Each Kafka record is a JSON object against a BGPRecord. Refer to https://bgpstream.caida.org/docs/api/pybgpstream/_pybgpstream.html for details on each data member of BGPRecord and BGPElem

Here is the JSON structure:

{
* project,
* collector,
* type,
* dump_time,
* time,
* status,
* dump_position,
* elements : [
    * element0 : {
        * type
        * time
        * peer_asn
        * peer_address
        * fields
    },
    * element1 : {
        * type
        * time
        * peer_asn
        * peer_address
        * fields
    },
        .
        .   
        .
    * elementN : {
        * type
        * time
        * peer_asn
        * peer_address
        * fields
    }

    
    ]
 }
        -  
    
