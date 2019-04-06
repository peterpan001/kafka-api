package com.lianjia.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: panli004
 * @date: 2019/2/27
 * Description: kafka生产者演示demo
 */
public class ProducerDemo {

    public static void main(String[] args) {
        /**判断消息是同步发送还是异步发送**/
        boolean isAsync = args.length==0 || !args[0].trim().equalsIgnoreCase("sync");

        /**对kafka进行配置**/
        Properties props = new Properties();
        /**kafka服务端的主机名和端口号**/
        props.put("bootstrap.servers","10.26.28.81:9092");
        /**客户端的id**/
        props.put("client.id","DemoProducer");
        /**利用序列化器将key和value的Java对象转化为字节数组**/
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        /**生产者的核心类**/
        KafkaProducer producer = new KafkaProducer<>(props);

        String topic = "lianjia-test-topic";
        /**消息的key**/
        int messageNo = 1;

        while(true){
            /**消息的value**/
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if(isAsync){
                /**异步发送消息**/
                //第一个参数是ProducerRecord类型的对象,封装了目标topic、消息的key、消息的value
                //第二个参数CallBack对象，当生产者接收到Kafka发送来的ACK确认消息的时候，会调用此CallBack对象的onCompletion()方法，实现回调功能
                producer.send(new ProducerRecord<>(topic,messageNo,messageStr), new DemoCallBack(startTime,messageNo,messageStr));
            }else{
                //同步发送消息
                try{
                    //KafkaProducer.send()方法的返回值类型是Future<RecordMetadata>,这里通过Future.get()方法，阻塞当前线程，等待Kafka服务端的ACK响应
                    producer.send(new ProducerRecord<>(topic,messageNo,messageStr)).get();
                    System.out.println("Sent message:("+messageNo+","+messageStr+")");
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            ++messageNo; //递增消息的key
        }
    }
}
