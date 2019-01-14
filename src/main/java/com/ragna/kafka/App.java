package com.ragna.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args){
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "localhost:9092");
        //properties.put("acks", "all");
        //properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);


        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try{
            for(int i = 0; i < 100; i++){
                System.out.println(i);
                //kafkaProducer.send(new ProducerRecord<String, String>("testcreate2", Integer.toString(i), "test message - " + i )).get();
                TestCallback callback = new TestCallback();
                ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                        "senz", "key-" + i, "message-"+i );
                kafkaProducer.send(data, callback);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }
}
