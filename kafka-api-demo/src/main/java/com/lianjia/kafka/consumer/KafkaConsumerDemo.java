package com.lianjia.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: panli004
 * @date: 2019/2/28
 * Description: Kafka消费者Demo演示
 */
public class KafkaConsumerDemo {

    public static void main(String[] args){
        //用properties集合对kafka进行配置
        Properties props = new Properties();
        //broker地址
        props.put("bootstrap.servers","10.26.28.81:9092");
        //所属consumer的group的id
        props.put("group.id","g2");
        //自动提交offset
        props.put("enable.auto.commit","true");
        //自动提交offset的时间间隔
        props.put("auto.commit.interval.ms","1000");
        //会话超时时间
        props.put("session.timeout.ms","30000");
        //key使用反序列化
        props.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        //value使用反序列化
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅lianjia-test-topic这个topic
        consumer.subscribe(Arrays.asList("lianjia-test-topic"));
        try{
            while(true){
                //从服务端拉取消息，每次poll()可以拉取多个消息
                ConsumerRecords<String, String> records = consumer.poll(100);
                //消费消息，这里仅仅是将消息的offset、key、value输出
                for(ConsumerRecord<String, String> record : records){
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            //关闭consumer
            consumer.close();
        }
    }
}
