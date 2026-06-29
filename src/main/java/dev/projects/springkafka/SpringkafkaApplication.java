package dev.projects.springkafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringkafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringkafkaApplication.class, args);
	}

	/**
	 * Spring Boot picks up any NewTopic beans and hands them to a KafkaAdmin,
	 * which on startup asks the broker to create them. The admin client uses an idempotent "create if not exists"
	 * call under the hood, so if greetings topic exists, the broker just shrugs and moves on.
	 */
	@Bean
	NewTopic greetings() {
		return TopicBuilder.name("greetings")
				.partitions(1)
				.replicas(1)
				.build();
	}

	@Bean
	ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> template.send("greetings", "Hello, Kafka!");
	}

	@KafkaListener(topics = "greetings", groupId = "demo")
	public void listen(String message) {
		System.out.println("Received Message: " + message);
	}
}
