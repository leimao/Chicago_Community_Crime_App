package edu.uchicago.leimao.kafka_simulated_crime;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;

import edu.uchicago.leimao.kafka_simulated_crime.CrimeRecord;
import edu.uchicago.leimao.kafka_simulated_crime.ChicagoCommunity;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// Inspired by http://stackoverflow.com/questions/14458450/what-to-use-instead-of-org-jboss-resteasy-client-clientrequest
public class CrimeArrivals {
	static class Task extends TimerTask {
		private Client client;
		Random rand = new Random();
		public List<CrimeRecord> generateRandomCrime(int numMaxCrimes) {
			// Generate an array of CrimeRocord
			// Generate random integer from 0 to numMaxCrimes
			int numCrimes = rand.nextInt(numMaxCrimes+1); 
			List<CrimeRecord> crimeList = new ArrayList<>();
			// Generate an array of the size
			for (int i = 0; i < numCrimes; i ++) {
				int year = 2018;
				int month = rand.nextInt(12) + 1;
				int day = rand.nextInt(28) + 1;
				String caseNumber = String.format("%05d", rand.nextInt(100000));
				String community = ChicagoCommunity.getRandomCommunity();
				CrimeRecord crimeInstance = new CrimeRecord(caseNumber, community, year, month, day);
				crimeList.add(crimeInstance);
			}
			return crimeList;
		}
		
		// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		Properties props = new Properties();
		String TOPIC = "leimao_crime_update";
		KafkaProducer<String, String> producer;
		
		public Task() {
			client = ClientBuilder.newClient();
			// enable POJO mapping using Jackson - see
			// https://jersey.java.net/documentation/latest/user-guide.html#json.jackson
			client.register(JacksonFeature.class); 
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}
		
		@Override
		public void run() {
			ObjectMapper mapper = new ObjectMapper();
			// At most 3 crimes per run
			List<CrimeRecord> newCrimes = generateRandomCrime(3);
			for (CrimeRecord crime : newCrimes) {
				ProducerRecord<String, String> data;
				try {
					data = new ProducerRecord<String, String>
					(TOPIC, 
					 mapper.writeValueAsString(crime));
					producer.send(data);
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	static String bootstrapServers = new String("localhost:9092");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 60*1000);
	}
}

