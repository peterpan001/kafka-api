package com.lianjia.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 新API的—生产者
 * @author panli
 */
public class CustomProducer {
    public static void main(String[] args)throws Exception {

       Properties props = new Properties();

        //kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "10.26.28.81:9092");
        //key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //等待所有副本节点的应答
        props.put("acks", "all");
        //设置发送区缓存的大小
        props.put("buffer.memory",33554432);
        //最大尝试次数
        props.put("retries",0);
        //一批消息处理大小
        props.put("batch.size",16384);
        //请求延时
        props.put("linger.ms",1);

        //实例化加载kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //kafka生产者生产消息
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>("lianjia-topic", Integer.toString(i), "hello kafka " + i));
            System.out.println("生产者发送消息....." + i);
        }
        System.out.println("生产者发送消息完毕");
        //关闭生产者
        producer.close();
    }
}
