package com.paterake.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTest {

  private static final String ZKHOST = "127.0.0.1";
  private static final String BROKERHOST = "127.0.0.1";
  private static final String BROKERPORT = "19092";
  private static final String TOPIC = "TEST_TOPIC";
  private static final int brokerId = 0;

  private EmbeddedZookeeper zkServer;
  private String zkConnect;
  private ZkClient zkClient;
  private ZkUtils zkUtils;
  private Properties propBroker;
  private KafkaServer kafkaServer;

  public void startZookeeper() {
    zkServer = new EmbeddedZookeeper();
    zkConnect = ZKHOST + ":" + zkServer.port();
    zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);
  }

  public KafkaConfig getKafkaConfig() throws IOException {
    propBroker = new Properties();
    propBroker.setProperty("zookeeper.connect", zkConnect);
    propBroker.setProperty("broker.id", String.valueOf(brokerId));
    propBroker.setProperty("log.dirs", Files.createTempDirectory("kafka_").toAbsolutePath().toString());
    propBroker.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
    return KafkaConfig.fromProps(propBroker);
  }

  public void startKafkaBroker() throws  IOException {
    KafkaConfig config = getKafkaConfig();
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);
    AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

    List<KafkaServer> clcnKafkaServer = new ArrayList<>();
    clcnKafkaServer.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(clcnKafkaServer), TOPIC , 0, 5000);
  }

  private Properties getProducerProperty() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    //properties.put("broker.id", brokerId);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    //properties.put("request.required.acks", "1");
    return properties;
  }

  private Properties getConsumerProperty() {
    Properties properties = new Properties();
    //properties.put("zookeeper.connect", zkConnect);
    properties.put("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    properties.put("group.id", "group1");
    properties.put("client.id", "consumer0");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("auto.offset.reset", "earliest");
    properties.put("enable.auto.commit", "false");
    return properties;
  }

  private KafkaConsumer<String, String> buildConsumer(String topicName) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperty());
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    List<TopicPartition> tps = Arrays.asList(tp);
    consumer.assign(tps);
    consumer.seekToBeginning(tps);
    //consumer.subscribe(Collections.singletonList(TOPIC));
    //consumer.subscribe(Arrays.asList(TOPIC));
    return consumer;
  }


  @Before
  public void setup() throws Exception {
    startZookeeper();
    startKafkaBroker();
  }

  @Test
  public void runTest() throws ExecutionException, InterruptedException {
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProducerProperty());
    KafkaConsumer<String, String> kafkaConsumer = buildConsumer(TOPIC);

    kafkaProducer.send(new ProducerRecord(TOPIC, "message")).get();
    //kafkaProducer.close();

    ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
    for (ConsumerRecord<String, String> record : records) {
      System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
    }
  }

  @After
  public void destroy() throws Exception {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }


}
