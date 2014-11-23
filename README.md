MapReduce
=========

The third project of 15-640 Distributed Systems. Design and implement a Map-Reduce facility, just like Hadoop. Developed with @bbfeechen

## About the system

The design is with certain constraints aimed at enabling it to work more efficiently in our computing environment with smaller data sets. It is capable of dispatching parallel maps and reduces across multiple hosts, as well as recovering from worker failure.

We also design and implement a file system KPFS just like HDFS to support our map-reduce system. It can support file replication and distributed file storage. 

More details about our system can be found in "kailianc-yangpan/Report_of_MapReduce_Facility.pdf".

## How to run the system

Running this map-reduce system is easy. You can run it in a single machine or multiple machines as you wish. Detailed steps can be found in "kailianc-yangpan/System_Administrator_Manual.pdf".

## Use our system

We simplify the interface for developers who want to use our system to do map-reduce work. Also, we provide two bash scripts to help developers write their own map-reduce classes. More details can be found in "kailianc-yangpan/Application_Programmer_Manual.pdf"