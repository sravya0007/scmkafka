//package net.scmlitekafka.springboot.kafka;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.OutputStreamWriter;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.bson.Document;
//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.serializer.JsonDeserializer;
//import org.springframework.stereotype.Component;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.MongoCredential;
//
//@Component
//public class Consumer {
//	
//	@Autowired
//	private ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;
//	
//	private MongoCollection<Document> coll = null;
//	private MongoClient mongo = null;
//	
////	   @KafkaListener(topics = "devices", groupId = "myGroup")
//	   public void Consume(String address, int port) throws JSONException
//	    {
//	        // establish a connection
//	    try
//	        {
//	        	
//	   //    MongoClient mongo = new MongoClient("localhost" , 27017);  
//	//       MongoClient mongo = new MongoClient(new MongoClientURI("mongodb://127.0.0.1:27017"));
//	//       MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
//	       mongo = new MongoClient();
//	       MongoDatabase db = mongo.getDatabase("ScmLite");  
//	       coll = db.getCollection("DeviceDataStream");
//	       
//	        }
//	    
//	    catch (Exception e) {
//	        // handle server down or failed query here.
//	    }
////	        catch(IOException i)
////	        {
////	            System.out.println();
////	        }
//	   
////	        catch(IOException i)
////	        {
////	            System.out.println(i);
////	        }	     	                       
//	        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory = kafkaListenerContainerFactory();
//	       
////	        kafkaListenerContainerFactory.get("devices", object.toString());
////	        System.out.println("sent data  ..."+object.toString());
//	        System.out.println("sent data  ..."+message);
//	        JSONArray array = new JSONArray(message);
//	        for (int i = 0; i < array.length(); i++) {
//	        	System.out.println("sent data  ..."+message);
//	        	 JSONObject object = array.getJSONObject(i); 
//	        	 Document doc = Document.parse(object.toString());
//	             coll.insertOne(doc);
//	           //  System.out.println(object.getString("Name"));  
//	        	
//	             System.out.println("received message= "+ message.toString());
//	        }       
//	    }
//	 
//	    public ConsumerFactory<String, String> consumerConfig() {
//	        Map<String, Object> config = new HashMap<>();
//	        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//	        config.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
//	        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//	        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//	        
//	        return new DefaultKafkaConsumerFactory<>(config);
//	    }
//
//	
//	    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//	        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//	        factory.setConsumerFactory(consumerConfig());
//	        
//	        return factory;
//	    }
////	    @KafkaListener(topics = "devices", groupId = "myGroup")
//	    public static void main(String args[]) throws JSONException
//	    {
//	    	 
//	    	Consumer consumer = new Consumer();
//	    	consumer.Consume("127.0.0.1", 27017);
//	    	
//	    }
//
//}


package net.scmlitekafka.springboot.kafka;
  
import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.ConsumerRecord;  
import org.apache.kafka.clients.consumer.ConsumerRecords;  
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;  
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Duration;  
import java.util.Arrays;  
import java.util.Collections;  
import java.util.Properties;  
@Component  
public class Consumer {  
	private static MongoCollection<Document> coll = null;
static 
	 String uri = "mongodb+srv://sravya98:hiheloS113@scmxpert.jkkrnjc.mongodb.net/SCM?retryWrites=true&w=majority";
    public static void main(String[] args) throws JSONException {
    	try
        {
        	
 //      MongoClient mongo = new MongoClient("localhost" , 27017);  
  //     MongoClient mongo = new MongoClient("localhost" , 27017);
       MongoClient mongoClient = MongoClients.create(uri);
       MongoDatabase db = mongoClient.getDatabase("SCM");  
 //      MongoCollection<Document> coll = db.getCollection("DeviceDataStream");
       coll = db.getCollection("DeviceData");
       System.out.println("Mongo connection estabished  ......");
       
        }
    
    catch (Exception e) {
        // handle server down or failed query here.
    }
        Logger logger= LoggerFactory.getLogger(Consumer.class.getName());  
        String bootstrapServers="127.0.0.1:9092";  
        String grp_id="group-id";  
        String topic="XYZ"; 
      
        //Creating consumer properties  
        Properties properties=new Properties();  
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);  
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);  
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  
        //creating consumer  
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(properties);  
//        Consumer consume = new Consumer();
 //       consume.coll();
 //       consume.coll = null;
        //Subscribing  
                consumer.subscribe(Arrays.asList(topic));  
        //polling  
        while(true){  
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){  
            	
                logger.info("Key: "+ record.key() + ", Value:" +record.value());  
                logger.info("Partition:" + record.partition()+",Offset:"+record.offset());            
                Document doc = Document.parse(record.value());
                coll.insertOne(doc);
               
            } 
            
  
  
        }  
     	         
    }  
    
}  
