package org.conan.kafka;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhanghang on 2014/9/17.
 */
public class SingleTopicPhoenix implements Runnable {
    private List<String> _replicaBrokers = new ArrayList<String>();
    private static String _topic;
    private String zkTopicOffset;

    private static long _maxReads;
    private static int _partition;
    private static List<String> _seeds;
    private static int _port;
    private static int fetchSize;
    private static ZkUtil _zk;
    private static ConsumerConfig conf;

    public SingleTopicPhoenix() {
        _maxReads = conf.maxReads();
        _partition = conf.partition();
        _seeds = conf.brokers();
        _port = conf.brokerPort();
        fetchSize = conf.fetchSize();
        _zk = new ZkUtil(conf.zkServer());
        zkTopicOffset = conf.getLastOffset(_topic,_partition);
        _replicaBrokers = new ArrayList<String>();
    }

    public static void main(String[] args) throws Exception {
        //集群模式
        String path;
        if (args != null && args.length == 2) {
            _topic = args[0];
            path = args[1];
        }
        else {
            _topic = "test";
            path = "src/main/resources/hadoop/kafkaToHdfs.properties";
        }
        conf = new ConsumerConfig(path);
        //创建单线程池
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(new Thread(new SingleTopicPhoenix()));
    }

    public void run() {
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(_seeds, _port, _topic, _partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + _topic + "_" + _partition;
        kafka.javaapi.consumer.SimpleConsumer consumer = new kafka.javaapi.consumer.SimpleConsumer(leadBroker, _port,100000, 64 * 1024, clientName);
        long readOffset = _zk.getLastOffset(zkTopicOffset);
        if (readOffset == 0) {
            readOffset = getLastOffset(consumer, _topic, _partition,kafka.api.OffsetRequest.LatestTime(), clientName);
        }

        int numErrors = 0;

        HdfsUtil hdfs = new HdfsUtil(conf.hdfs(),conf.job());
        //String hdfsTopicPath = conf.hdfsTopicPath(_topic);
        while (_maxReads > 0) {
            if (consumer == null) {
                consumer = new kafka.javaapi.consumer.SimpleConsumer(leadBroker, _port, 100000,64 * 1024, clientName);
            }
            // Note: this fetchSize of 100000 might need to be increased if
            // large batches are written to Kafka
            FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(_topic, _partition, readOffset, fetchSize).build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(_topic, _partition);
                System.out.println("Error fetching data from the Broker:"
                        + leadBroker + " Reason: " + code);
                if (numErrors > 5)
                    break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for
                    // the last element to reset
                    readOffset = getLastOffset(consumer, _topic, _partition,
                            kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker, _topic, _partition,_port);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                continue;
            }
            numErrors = 0;
            long numRead = 0;

            //logger.info("Waiting for message ...");
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
                    _topic, _partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset
                            + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String mes = null;
                try {
                    mes = new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                Long offset = messageAndOffset.offset();
                //TODO 中文乱码
                System.out.println(_topic+": "+String.valueOf(offset)+mes);
                //apend to hdfs
                PhoenixUtil pu = new PhoenixUtil(_topic,offset);
                _zk.setOffset("/hdfs/" + conf.groupId() + "/" + _topic + "/" + _partition, readOffset);
                numRead++;
                //_maxReads--;
            }
            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.print("endA");
        if (consumer != null)
            System.out.print("endB");
            consumer.close();
    }

    public static long getLastOffset(kafka.javaapi.consumer.SimpleConsumer consumer, String topic,int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: "+ response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(String a_oldLeader, String _topic,
                                 int _partition, int _port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(_replicaBrokers, _port,
                    _topic, _partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
                    && i == 0) {
                // first time through if the leader hasn't changed give
                // ZooKeeper a second to recover
                // second time, assume the broker did recover before failover,
                // or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out
                .println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception(
                "Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<String> a_seedBrokers,int _port, String _topic, int _partition) {
        PartitionMetadata returnMetaData = null;
        loop: for (String seed : a_seedBrokers) {
            kafka.javaapi.consumer.SimpleConsumer consumer = null;
            try {
                consumer = new kafka.javaapi.consumer.SimpleConsumer(seed, _port, 100000, 64 * 1024,"leaderLookup");
                List<String> topics = Collections.singletonList(_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == _partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed
                        + "] to find Leader for [" + _topic + ", "
                        + _partition + "] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        if (returnMetaData != null) {
            _replicaBrokers.clear();
            for (Broker replica : returnMetaData.replicas()) {
                _replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}