# Introduction

Send kafka message to hdfs using api not mapreduce.
In order to fetch messages from kafka to hdfs gracefully,
I chose kafka and hdfs api not mapreduce, in which
you can fetch one topic with one partition or all partitions,
and also can you fetch all topics with all partitions,if you like.

# QUICK START

1.mvn package	

2.create a consumer.properties like this in the resourses/hadoop	

3.hadoop jar kafkaHdfs-1.0-SNAPSHOT-jar-with-dependencies.jar org.conan.kafka.SingleTopicConsumer $topic $path/consumer.properties
or hadoop jar kafkaHdfs-1.0-SNAPSHOT-jar-with-dependencies.jar org.conan.kafka.AlltopicsConsumer path/consumer.properties
