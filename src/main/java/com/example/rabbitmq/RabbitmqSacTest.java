package com.example.rabbitmq;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;

@SpringBootApplication
public class RabbitmqSacTest {

    private static final boolean DURABLE = true;
    private static final String MY_QUEUE_NAME = "myQueue";

    public static void main(String[] args) {
        SpringApplication.run(RabbitmqSacTest.class, args);
    }

    @Bean
    public ApplicationRunner runner(RabbitTemplate template) {
        return args -> {
            byte[] array = new byte[7]; // length is bounded by 7
            new Random().nextBytes(array);
            String randomName = new String(array, Charset.forName("UTF-8"));

            // Using a random string to differentiate which instance published the message.
            for (int i = 0; i<20; i++) {
                final String payload = randomName+": "+i;
                System.out.println("publishing: "+payload);
                template.convertAndSend("myQueue", payload);
            }
        };
    }

    @Bean
    public Queue myQueue() {
        final Map<String, Object> args = Map.of("x-single-active-consumer", true);

        return new Queue(MY_QUEUE_NAME, DURABLE, false, false, args);
    }

    @RabbitListener(queues = MY_QUEUE_NAME)
    public void listen(String in) throws InterruptedException {
        System.out.println("Received message from myQueue: " + in);
        
        Thread.sleep(10000);

        System.out.println("Processed message from myQueue: " + in);
    }


}
