package com.lianjia.kafka.demo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 控制分区
 * @author panli
 */
public class CustomPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 1;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
