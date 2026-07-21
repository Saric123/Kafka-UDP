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


After running kafka, sensor nodes are run and waiting for Coordinator to produce start message on topic. Right after startUp, sensors subscribe to the topics which are used by Coordinator for sending messages. 
40 seconds after sending starting message, Coordinator sends stop message on same topic on which sensors end their work.

Each sensor establishes communication with all neighbor nodes using UDP protocol. They exchange their data with neighbor sensors and calculate mean value in window of 5 seconds. 

After the node receives the control message "Start", it sends a registration message on the topic "Register" in
JSON format. The registration message consists of the node identifier, its IP address and port.
After a node receives registration messages containing the identifiers of all other nodes via topic
"Register", the node starts communication with all other nodes using the UDP protocol.
To simulate packet loss in the network, SimpleSimulatedDatagramSocket class is used.

After the node generates its own reading, it creates a UDP packet containing the generated reading together with
updated vector and updated scalar time stamp, and sends it via UDP to all other nodes in the network.
For each successfully received data packet, node X sends an acknowledgment to node Y, also in the form of a packet.
Node Y Node Y ˇ waits for acknowledgments for all packets it sends. Node Y waits for acknowledgments for all packets it sends.
The acknowledgment have a simpler format than the data packet. Each node uses the same port to receive acknowledgments and data packets.

If a node does not receive an acknowledgment for a sent data packet, it resends that data packet. The original timestamps must be preserved.
After 5 seconds, each node sorts all readings (its own and those received from other nodes in the network) from the preceding 5-second interval using both vector and scalar timestamps.
The timestamps are then printed to the console alongside the sorted readings. It also calculates and prints the average value of the received readings for that 5-second window. The sorting is performed using both timestamps, and each sorted set must be printed individually.



