package com.ragna.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("senz");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                System.out.println("entra al while");
                ConsumerRecords records = kafkaConsumer.poll(10);

                Iterator<ConsumerRecord> iterator = records.iterator();
                System.out.println("crea el iterador");
                while(iterator.hasNext()){

                    ConsumerRecord record = iterator.next();
                    System.out.println("pasa el next");
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }


                //for (ConsumerRecord record: records){


                 //   System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                //}
                Thread.sleep(3000);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}