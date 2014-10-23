package org.conan.kafka;

import org.apache.hadoop.mapred.JobConf;

import java.io.FileInputStream;
import java.io.IOException;

import java.util.*;

/**
 * Created by zhanghang on 2014/9/17.
 */
public class ConsumerConfig{
    private Properties _props = new Properties();

    public ConsumerConfig(String _conf_path) {
        try {
            _props.load(new FileInputStream(_conf_path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String hdfsTopicPath(String topic,int partition) {
        return _props.getProperty("hdfs.topic.dir")+ "/"+topic+"/"+partition;
    }
    public String hdfs() {
        return _props.getProperty("hdfs.url.path");
    }

    public String zkServer() {
        return _props.getProperty("zookeeper.connect");
    }

    public Long maxReads() {
        return Long.parseLong(_props.getProperty("kafka.consumer.maxReads"));
    }

    public List<String>brokers() {
        String[] brokers = _props.getProperty("kafka.brokers").split(",");
        return Arrays.asList(brokers);
    }

    public int brokerPort() {
        String port = _props.getProperty("kafka.brokers.port");
        int brokerPort = 9092;
        if (null != port) {
            try {
                brokerPort = Integer.parseInt(port);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return brokerPort;
    }

    public int partition() {
        return Integer.parseInt(_props.getProperty("kafka.consumer.partition"));
    }

    public int fetchSize(){
        return Integer.parseInt(_props.getProperty("kafak.consumer.fetchSize"));
    }

    public boolean initOffset() {
        return Boolean.parseBoolean(_props.getProperty("kafka.consumer.initOffset"));
    }

    public String groupId() {
        return _props.getProperty("kafka.consumer.groupId");
    }

    public String getLastOffset(String topic,int partition) {
        //zk结构：/hdfs/groupid/topics/partition/offset
        return "/hdfs/"+groupId()+"/"+partition+"/"+topic;
    }

    public JobConf job() {
        JobConf conf = new JobConf(HdfsUtil.class);
        conf.setJobName(_props.getProperty("job.name"));
        conf.addResource(_props.getProperty("job.config.core"));
        conf.addResource(_props.getProperty("job.config.hdfs"));
        conf.addResource(_props.getProperty("job.config.mapred"));
        return conf;
    }
}