/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.jumpingbean.rabbitconsumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mark
 */
public class Main {

    public static void main(String[] args) throws IOException, TimeoutException {

        if (args.length==0){
            throw new RuntimeException("Please provide a routing key and an optional queue name and optional hostname");
        }
        
        HashMap<String,String> map = new HashMap<>();
        map.put("rabbit1","172.20.0.4");
        map.put("rabbit2", "172.20.0.2");
        map.put("rabbit3","172.20.0.3");
        String EXCHANGE_NAME = "Direct1";
        String QUEUE_NAME = "demoq";

        String HOST=map.get("rabbit1");
        if (args.length>2) {
            HOST = map.get(args[2]);
        }        
        
        String routingKey =args[0];
        if(args.length>1){
            QUEUE_NAME=args[1];
        }
        if (args.length>2){
            HOST=map.get(args[2]);
        }
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername("mark");
        factory.setPassword("pass");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,routingKey);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }

}
