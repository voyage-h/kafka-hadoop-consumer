package org.conan.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * Created by zhanghang on 2014/9/14.
 */
public class HighLevelConsumer implements MessageExecuter{
    private static org.conan.kafka.ConsumerConfig conf;
    private static String TOPIC;
    private String _offset;
    private ConsumerConnector connector;
    private static String _conf_path;
    private ExecutorService threadPool;
    private static int _partition;
    private ConsumerConfig config;


    public HighLevelConsumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect", conf.zkServer());
        props.put("group.id", conf.groupId());
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        config = new ConsumerConfig(props);
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        //集群模式
        if (args != null && args.length == 2) {
            TOPIC = args[0];
            _conf_path = args[1];
        }
        else {
            TOPIC = "test";
            _conf_path = "src/main/resources/hadoop/kafkaToHdfs.properties";
        }
        conf = new org.conan.kafka.ConsumerConfig(_conf_path);
        _partition = conf.partition();
        HighLevelConsumer consumer = null;
        try {
            consumer = new HighLevelConsumer();
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(consumer == null){
                consumer.close();
            }
        }
    }

    public void start() throws Exception {

        connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put(TOPIC, _partition);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
        List<KafkaStream<byte[], byte[]>> partitions = streams.get(TOPIC);
        threadPool = Executors.newFixedThreadPool(_partition);
        for (KafkaStream<byte[], byte[]> partition : partitions) {
            threadPool.execute(new MessageRunner(partition));
        }
    }

    public void close() {
        try {
            threadPool.shutdownNow();
        } catch (Exception e) {
        } finally {
            connector.shutdown();
        }
    }
    class MessageRunner implements Runnable {
        private KafkaStream<byte[], byte[]> partition;

        MessageRunner(KafkaStream<byte[], byte[]> partition) {
            this.partition = partition;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = partition.iterator();

            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> item = it.next();
                System.out.println("==========================");
                System.out.println("partiton:" + item.partition());
                System.out.println("offset:" + item.offset());
                _partition = item.partition();
                _offset = String.valueOf(item.offset());
                String mgs;
                try {
                    mgs = new String(item.message(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    mgs = new String(item.message());
                }
                System.out.println("message:" + mgs);
                try {
                    execute(mgs+"\n");// UTF-8
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void execute(String message) throws IOException, KeeperException, InterruptedException {
        HdfsUtil hdfs = new HdfsUtil(conf.hdfs(),conf.job());
        // hdfs.append(conf.hdfsTopicPath(TOPIC), message);

    }
}
