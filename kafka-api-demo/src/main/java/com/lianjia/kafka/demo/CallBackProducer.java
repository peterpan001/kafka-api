package com.lianjia.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 创建生产者带回调函数
 * @author panli
 */
public class CallBackProducer {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();

        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "10.26.28.81:9092");
        // 等待所有副本节点的应答
        props.put("acks", "1");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 50; i++) {
            Thread.sleep(500);
            kafkaProducer.send(new ProducerRecord<String, String>("lianjia01", "hh" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println(metadata.partition() + "---" + metadata.offset());
                    }
                }
            });
        }

        kafkaProducer.close();
    }

}
