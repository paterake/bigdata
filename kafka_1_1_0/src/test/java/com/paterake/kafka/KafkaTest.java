package com.paterake.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.kafka.common.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

import static org.junit.Assert.assertEquals;

public class KafkaTest {

  private static final String ZKHOST = "127.0.0.1";
  private static final String BROKERHOST = "127.0.0.1";
  private static final String BROKERPORT = "19092";
  private static final String TOPIC = "TEST_TOPIC";

  private String zkConnect;
  private ZkUtils zkUtils;
  private EmbeddedZookeeper zkServer;
  private ZkClient zkClient;
  private KafkaServer kafkaServer;

  private void startZookeeper() {
    zkServer = new EmbeddedZookeeper();
    zkConnect = ZKHOST + ":" + zkServer.port();
    zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);
    System.out.println("Zookeeper started");
  }

  private void startKafkaBroker() throws IOException {
    Properties brokerProps = new Properties();
    brokerProps.setProperty("zookeeper.connect", zkConnect);
    brokerProps.setProperty("broker.id", "0");
    brokerProps
        .setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
    brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
    brokerProps.setProperty("offsets.topic.replication.factor", "1");
    KafkaConfig config = new KafkaConfig(brokerProps);
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);

    AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

    List<KafkaServer> clcnKafkaServer = new ArrayList<>();
    clcnKafkaServer.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(
        scala.collection.JavaConversions.asScalaBuffer(clcnKafkaServer), TOPIC, 0, 5000);

    System.out.println("Broker started");
  }

  public KafkaProducer<Integer, byte[]> startKafkaProducer() {
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    producerProps
        .setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    producerProps.setProperty("value.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(producerProps);
    return producer;
  }

  private KafkaConsumer<Integer, byte[]> startKafkaConsumer() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
    consumerProps.setProperty("group.id", "group0");
    consumerProps.setProperty("client.id", "consumer0");
    consumerProps.setProperty("key.deserializer",
        "org.apache.kafka.common.serialization.IntegerDeserializer");
    consumerProps.setProperty("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put("auto.offset.reset", "earliest"); // starts at beginning of topic
    KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<Integer, byte[]>(consumerProps);
    consumer.subscribe(Arrays.asList(TOPIC));
    return consumer;
  }

  @Before
  public void setup() throws Exception {
    startZookeeper();
    startKafkaBroker();
  }

  @After
  public void destroy() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  @Test
  public void producerTest() {
    KafkaProducer<Integer, byte[]> producer = startKafkaProducer();
    KafkaConsumer<Integer, byte[]> consumer = startKafkaConsumer();

    ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC, 42,
        "test-message".getBytes(StandardCharsets.UTF_8));
    producer.send(data);
    producer.close();

    ConsumerRecords<Integer, byte[]> records = consumer.poll(5000);
    assertEquals(1, records.count());
    Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
    ConsumerRecord<Integer, byte[]> record = recordIterator.next();
    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(),
        new String(record.value(), StandardCharsets.UTF_8));
    assertEquals(42, (int) record.key());
    assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));
    consumer.close();
    producer.close();
  }

}
