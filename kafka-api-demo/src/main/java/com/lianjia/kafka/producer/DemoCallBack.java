package com.lianjia.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: panli004
 * @date: 2019/2/27
 * Description: 回调对象
 */
public class DemoCallBack implements Callback {

    //开始发送消息的时间戳
    private final long startTime;
    //消息的key
    private final int key;
    //消息的value
    private final String message;

    public DemoCallBack(long startTime, int key, String message){
        this.key = key;
        this.startTime = startTime;
        this.message = message;
    }

    /**
     * 生产者发送成功消息，收到kafka服务端发来的ACK确认消息后，会调用此回调函数
     * @param metadata 生产者发送消息的元数据，如果发送过程中出现异常，此参数为null
     * @param e 发送过程中出现的异常，如果发送成功，则此参数为null
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        long elapsedTime = System.currentTimeMillis()-startTime;
        if(metadata!=null){
            /**RecordMetadata包含了分区信息、offset信息等**/
            System.out.println("message（" + key + "," + message +") sent to partiotion("+metadata.partition()+"),"+
                    "offset("+metadata.offset()+") in" + elapsedTime + "ms");
        }else{
            e.printStackTrace();
        }
    }
}
