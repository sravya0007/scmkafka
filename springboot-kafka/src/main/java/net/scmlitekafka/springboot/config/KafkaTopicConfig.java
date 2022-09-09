package net.scmlitekafka.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
	
	
	@Bean
	public NewTopic scmliteTopic(){
		
		return TopicBuilder.name("testtopic")
				.build();
	}
	
	@Bean
	public NewTopic scmliteJsonTopic(){
		
		return TopicBuilder.name("testtopic_json")
				.build();
	}

}
 