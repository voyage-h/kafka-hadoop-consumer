package org.conan.kafka;

/**
 * Created by zhanghang on 2014/9/18.
 */
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.put("zk.connect", "zk-1:2181,zk-2:2181,zk-3:2181");
        properties.put("metadata.broker.list", "kafka-1:9092,kafka-2:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        // 构建消息体
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>("zz", "试试中文");
        producer.send(keyedMessage);

        Thread.sleep(1000);

        producer.close();
    }

}