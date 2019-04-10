# kafka-producer-bgp
Push BGP data to Kafka

### Record Structure
Each record is a JSON object for a BGPRecord. Refer to https://bgpstream.caida.org/docs/api/pybgpstream/_pybgpstream.html for details on each data member of BGPRecord and BGPElem

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
    
