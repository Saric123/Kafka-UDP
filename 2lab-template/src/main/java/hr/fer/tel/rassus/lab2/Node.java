package hr.fer.tel.rassus.lab2;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import hr.fer.tel.rassus.stupidudp.network.EmulatedSystemClock;
import hr.fer.tel.rassus.stupidudp.network.SimpleSimulatedDatagramSocket;

class NodeClient extends Thread {
	private final static int NO2ColumnFromCSVFile = 4;
	Node node;
	private static final String readingsFilePath = "readings.csv";
	List<String> lines;
	
	public NodeClient(Node node) {
		super();
		this.node = node;
		try {
			lines = Files.readAllLines(Paths.get(readingsFilePath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void printAllData() {
		double sum = 0.0;
		System.out.println("PRINTING READINGS AFTER 5 SECONDS");
		System.out.println("SORTING READINGS WITH RESPECT TO SCALAR TIME MARK");
		node.listOfReadings.sort(Comparator.comparingInt((d) -> d.getScalarTimeMark()));
		
		for(var reading : node.listOfReadings) {
			sum += Integer.parseInt(reading.getMeasure());
			System.out.println("( " + reading.getMeasure() + ", " + reading.getScalarTimeMark() +" )");
		}
		
		Comparator<Reading> vectorTimeMarkComparator = (a, b) -> {
			int flag = -1;
			int[] firstVector = a.getVectorTimeMark();
			int[] secondVector = b.getVectorTimeMark();
		    for (int i = 0; i < firstVector.length; i++) {
		        if(firstVector[i] < secondVector[i]) {
		        	flag = -1;
		        } else if(firstVector[i] > secondVector[i]) {
		        	flag = 1;
		        }
		        
		    }
		    return flag;
		};
		
		node.listOfReadings.sort(vectorTimeMarkComparator);
		System.out.println("SORTING READINGS WITH RESPECT TO VECTOR TIME MARK");
		for(var reading : node.listOfReadings) {
			System.out.println("( " + reading.getMeasure() + ", " + Arrays.toString(reading.getVectorTimeMark()) +" )");
		}
		
		System.out.println("---- MEAN VALUE AFTER 5 SECOND INTERVAL ------");
		System.out.println("MEAN === " + (sum/node.listOfReadings.size()));
	}
	
	public byte[] encodeReading(Reading reading) {
		//int capacity = 4 + 4 + 4 * Node.TotalNumberOfSensors + reading.getMeasure().getBytes(StandardCharsets.UTF_8).length;
		int capacity = 25;
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		byte[] measureBytes = reading.getMeasure().getBytes(StandardCharsets.UTF_8);
	    buffer.putInt(measureBytes.length);
	    buffer.put(measureBytes);
	    buffer.putInt(reading.getScalarTimeMark());
	    for(int i : reading.getVectorTimeMark()) {
	    	buffer.putInt(i);
	    }
	    
	    return buffer.array();
	}

	public boolean checkForStopMessage() {
		ConsumerRecords<String, String> consumerRecords = node.commandConsumer.poll(Duration.ofMillis(300));
    	if(!consumerRecords.isEmpty()) {
    		for(var record : consumerRecords) {

    			if(record.topic().equals(node.TOPIC1) && record.value().equals(node.STOP)) {
    				System.out.println("I HAVE GOT A TOPIC1 MESSAGE AND STOPPAGE MESSAGE");
    				return true;
    			}
    		}
    		node.commandConsumer.commitAsync();
    	}
    	else {
    		System.out.println("DIDNT GET STOP MESSAGE");
    		return false;
    	}
    	return false;
	}

	@Override
	public void run() {
		long FiveSecondTimer = System.currentTimeMillis();
		while(Node.running) {
			int row = (int)((System.currentTimeMillis() - node.clock.getStartTime())/1000) % 100 + 1;
			String[] measures = lines.get(row).split(",", -1);

			String myMeasure = measures[NO2ColumnFromCSVFile];
			myMeasure = myMeasure == "" ? "0" : myMeasure;
			node.myScalarMark += node.clock.elapsedTime();
			node.myVectorMark[node.ID]++;
			Reading myReading = new Reading(myMeasure, node.myScalarMark, node.myVectorMark);
			node.listOfReadings.add(myReading);
			//System.out.println("MY READING " + myReading.toString());

			DatagramPacket packetSend;

			//start to work as UDP client
			for(Entry<Integer, Integer> entry : node.neighbors.entrySet()) {
				int serverPort = entry.getValue(); 

				InetAddress address = null;
				try {
					address = InetAddress.getByName("localhost");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				
				node.myVectorMark[node.ID]++;
				myReading.setVectorTimeMark(node.myVectorMark);
				while(true && Node.running) {
					boolean stop = checkForStopMessage();
					if(stop) {
						System.out.println("STOPPING CLIENT SIDE AND SENDING INFO TO SERVER SIDE");
						Node.running = false;
						break;
					}
					try {
						byte[] sendBuf = encodeReading(myReading);
						packetSend = new DatagramPacket(sendBuf, sendBuf.length,
								address, serverPort);
						
						node.socket.send(packetSend);
						System.out.println(node.ID + " sends reading to " + entry.getKey());
						Integer ack = node.ackQueue.poll(3, TimeUnit.SECONDS);
						
						node.myScalarMark += node.clock.elapsedTime();
						if(ack != null) {
							System.out.println(node.ID + "GOT AN ACK FROM " + entry.getKey() +" THAT READING IS DELIVERED");
							break;
						}

					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}

			}
			
			if((System.currentTimeMillis() - FiveSecondTimer)/1000 >= 5) {
				
				printAllData();
				FiveSecondTimer = System.currentTimeMillis();
			}
			
		}
		
	}
}


public class Node {
	private static final Logger logger = Logger.getLogger(Node.class.getName());
	protected static final int TotalNumberOfSensors = 3;
	protected BlockingQueue<Integer> ackQueue = new LinkedBlockingQueue<>();

	
	protected int ID, PORT;
	protected static String TOPIC1 = "Command";
	private static String TOPIC2 = "Register";
	private static String START = "Start";
	protected static String STOP = "Stop";
	protected Consumer<String, String> commandConsumer;
	protected Consumer<String, String> consumer;
	private Producer<String, String> producer;
	protected Map<Integer, Integer> neighbors = new HashMap<>();
	
	protected List<Reading> listOfReadings = Collections.synchronizedList(new ArrayList<>());
	protected DatagramSocket socket;
	
	
	protected static volatile boolean running = true;

	protected int myScalarMark = 0;
	protected int[] myVectorMark = new int[TotalNumberOfSensors];
	
	protected EmulatedSystemClock clock;
	public Node() {
		clock = new EmulatedSystemClock();
	}

	private void printNeighbors() {
		System.out.println("Neighbors of ID " + this.ID);
		for(Entry<Integer, Integer> entry : neighbors.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}
	
	private void setNode() throws SocketException, IllegalArgumentException {
		Scanner sc = new Scanner(System.in);
		logger.info("SET NODE ID AND UDP SERVER PORT");
		this.ID = sc.nextInt();
		this.PORT = sc.nextInt();
		sc.close();
		socket = new SimpleSimulatedDatagramSocket(PORT, 0.3, 1000);
	}
	
	private void defineCommandConsumer() {
		Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Sensor"+ this.ID + TOPIC1);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        commandConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
        
        commandConsumer.subscribe(List.of(TOPIC1));
        
		
	}
	
	private void defineRegisterConsumer() {
		Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Sensor"+ this.ID + TOPIC2);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
        
        consumer.subscribe(List.of(TOPIC2));
        
		
	}
	
	
	private void waitForCoordinatorMessage() {
		while(true) {
        	ConsumerRecords<String, String> consumerRecords = commandConsumer.poll(Duration.ofMillis(500));
        	if(!consumerRecords.isEmpty()) {
        		for(var record : consumerRecords) {
        			
        			if(record.topic().equals(TOPIC1) && record.value().equals(START)) {
        				logger.info("I HAVE GOT A TOPIC1 START MESSAGE");
        				return;
        			}
        		}
        		this.commandConsumer.commitAsync();
        	} else {
        		logger.info("I GOT NOTHING");
        		try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        }
	}
	private void defineProducer() {
		Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);
        JsonObject json = new JsonObject();
        json.addProperty("id", String.valueOf(this.ID));
        json.addProperty("address", "localhost");
        json.addProperty("port", String.valueOf(this.PORT));
        
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC2, null, json.toString());
        producer.send(record);
        producer.flush();
        System.out.println("I with ID of " + ID + " send json message " + json.toString() + " to topic " + TOPIC2);
	}
	
	private void getAllNodes() {
		while(this.neighbors.size() < TotalNumberOfSensors-1) {
			System.out.println("SEARCHING FOR NODES");
			ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(1000));
			
			for(var record : consumerRecords) {
				if(record.topic().equals(TOPIC2)) {
					JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
					if(jsonObject.get("id").getAsInt() != this.ID) {
						neighbors.put(jsonObject.get("id").getAsInt(), jsonObject.get("port").getAsInt());
					}
				}
			}
			printNeighbors();
			
		}
		System.out.println("ENDED SEARCH FOR NODES");
	}
	
	public Reading decodeReading(byte[] bytes) {
		Reading reading;
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		int len = buffer.getInt();
		
		byte[] measure = new byte[len];
		
		
		buffer.get(measure);
		String mes= new String(measure, StandardCharsets.UTF_8);
		
		int scalarTimeMark = buffer.getInt();
		int[] vector = new int[3];
		for(int i = 0; i < TotalNumberOfSensors; i++) {
			vector[i] = buffer.getInt();
		}
		
		reading = new Reading(mes, scalarTimeMark, vector);
		
		
		return reading;
	}
	
	public void updateMyScalarAndVectorMark(Reading reading) {
		if(reading.getScalarTimeMark() > myScalarMark) {
			myScalarMark = 1+reading.getScalarTimeMark();
		}
		myVectorMark[ID]++;
		int[] array = reading.getVectorTimeMark();
		for(int i = 0; i < TotalNumberOfSensors; i++) {
			if(i != ID && myVectorMark[i] < array[i]) {
				myVectorMark[i] = array[i];
			}
		}
	}
 	
	private void startServer() throws IOException {
		byte[] rcvBuf = new byte[25]; // received bytes
		
        byte[] sendBuf = "ACK".getBytes();// sent bytes
		socket.setSoTimeout(500);
		while(Node.running) {
			Arrays.fill(rcvBuf, (byte) 0);
		
			// create a DatagramPacket for receiving packets
            DatagramPacket packet = new DatagramPacket(rcvBuf, rcvBuf.length);
            
            try {
            	// receive packet
            	socket.receive(packet);
            } catch(IOException e) {
            	continue;
            }
            
            if(packet.getLength() <= 3) {
            	ackQueue.offer(1);
            	System.out.println(ID + "GOT AN ACK ON SERVER SIDE FROM " + packet.getPort() + " AND IS NOTIFYING CLIENT SIDE");
            	continue;
            }
            byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
            Reading reading = decodeReading(data);
            updateMyScalarAndVectorMark(reading);
            listOfReadings.add(reading);
            myScalarMark += clock.elapsedTime();
            
            final DatagramPacket confirmMessage = new DatagramPacket(sendBuf,sendBuf.length,
            									  packet.getAddress(), packet.getPort());
            
            socket.send(confirmMessage);
            System.out.println(ID + " IS SENDING ACK TO " + packet.getPort());
            if(!Node.running) {
            	System.out.println("PRINTING READINGS AT THE END OF WORK");
            	listOfReadings.sort(Comparator.comparingInt((d) -> d.getScalarTimeMark()));
        		for(var r : listOfReadings) {
        			System.out.println("( " + r.getMeasure() + ", " + r.getScalarTimeMark()
        									+ Arrays.toString(r.getVectorTimeMark()) +" )");
        		}
        		break;
            }
		}
		System.out.println("STOPPING SERVER SIDE");
		/*
		 * for safe stoppage purposes wait until the client side is closed 
		 * and then close server side
		 */
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		socket.close();
		
	}
	
	public static void main(String[] args) throws IOException {
		Node node = new Node();
		node.setNode();
		node.defineCommandConsumer();
		node.defineRegisterConsumer();
        System.out.println("Waiting for start message from Coordinator on topic " + TOPIC1);
        node.waitForCoordinatorMessage();
        node.defineProducer();
        node.getAllNodes();
        node.printNeighbors();
        //start client side of the node
        NodeClient nodeClient = new NodeClient(node);
        nodeClient.start();
        // start server side of a node 
        System.out.println("STARTING SERVER");
        node.startServer();
        
	}

}
