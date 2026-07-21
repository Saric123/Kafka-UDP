package hr.fer.tel.rassus.lab2;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Coordinator {
	private static String TOPIC = "Command";
	private static String controlMessageStart = "Start";
	private static String controlMessageStop = "Stop";
	public static void main(String[] args) {
		
		Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);
		
        
        ProducerRecord<String, String> recordStart = new ProducerRecord<>(TOPIC, null, controlMessageStart);
        
        producer.send(recordStart);
        producer.flush();
        System.out.println("I SENT A TOPIC MESSAGE (START)");
        
        try {
			Thread.sleep(40000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        ProducerRecord<String, String> recordStop = new ProducerRecord<>(TOPIC, null, controlMessageStop);
        
        producer.send(recordStop);
        producer.flush();
        System.out.println("I SENT A TOPIC MESSAGE (STOP)");
        producer.close();
        
	}

}
