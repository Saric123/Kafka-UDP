# Kafka-UDP

 A distributed system is used for tracking sensor readings over time with the help of a group of peer nodes, where each node “acts” as a sensor. The nodes are controlled using control messages sent by the coordinating Kafka node (i.e., the coordinator). The nodes connect directly to all other nodes in the network, forming a fully connected network.

The task of the coordinator and the Kafka server is to coordinate the formation of a fully connected network of nodes — that is, to enable the exchange of identifiers between nodes (so that nodes can connect to each other) and to start and stop node operation once the network topology has been established.

The task of the nodes is to exchange sensor readings with all neighboring nodes, and to sort the received and local readings in time using scalar and vector timestamps, as well as to compute the average value of the readings within a 5‑second time window.

The sensors communicate with each other using the UDP protocol, meaning each sensor is both a UDP client and a UDP server. Additionally, each UDP connection simulates UDP packet loss in the network. Every lost UDP packet must be sent again — in other words, retransmission must be implemented.

Download newest version of Kafka https://kafka.apache.org/community/downloads/. Extract archive to known folder e.g KAFKA_HOME = C:\kafka. Kafka is configured using file server.properties which is located in dir: KAFKA_HOME\config. Kafka stores content to the location defined inside of server.properties under log.dirs variable. Value of log.dirs can be arbitrary. 

Kafka startup:
  It is started from terminal. Firstly, it is needed to generate Cluster ID and define storage. Scripts for that and Kafka startup are located inside KAFKA_HOME\bin\windows. 

Generate ID : C:\kafka\bin\windows\kafka-storage.bat random-uuid

Define storage: .\bin\windows\kafka-storage.bat format --standalone -t [random-uuid] -c C:\kafka\config\server.properties

Run kafka: .\bin\windows\kafka-server-start.bat config\server.properties


