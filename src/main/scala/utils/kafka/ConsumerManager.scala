package utils.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization._
import utils.Configuration

import scala.collection.JavaConverters._


object ConsumerManager {

  lazy val consumerStringByteArray: Consumer[String, Array[Byte]] = createConsumerStringByteArray()
  lazy val consumerLongByteArray: Consumer[Long, Array[Byte]] = createConsumerLongByteArray()

  def getCommonProperties : Properties = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props
  }

  def createConsumerStringByteArray(): Consumer[String, Array[Byte]] = {

    val props = getCommonProperties

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[String, Array[Byte]](props)
  }

  def createConsumerLongByteArray(): Consumer[Long, Array[Byte]] = {

    val props = getCommonProperties

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new LongDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[Long, Array[Byte]](props)
  }

  def listConsumerStringByteArrayTopics(): Unit = {

    val topics: util.Map[String, util.List[PartitionInfo]] = consumerStringByteArray.listTopics()
    for (topicName <- topics.keySet().toArray()) {

      if (!topicName.toString.startsWith("__")) {

        val partitions: util.List[PartitionInfo] = topics.get(topicName)
        for (partition <- partitions.toArray()) {
          println("Topic: " +
            topicName + "; Partition: " + partition)
        }
      }
    }
  }

  def listConsumerLongByteArrayTopics(): Unit = {

    val topics: util.Map[String, util.List[PartitionInfo]] = consumerLongByteArray.listTopics()
    for (topicName <- topics.keySet().toArray()) {

      if (!topicName.toString.startsWith("__")) {

        val partitions: util.List[PartitionInfo] = topics.get(topicName)
        for (partition <- partitions.toArray()) {
          println("Topic: " +
            topicName + "; Partition: " + partition)
        }
      }
    }
  }

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeConsumerStringByteArrayToTopic(topic: String): Unit =
    consumerStringByteArray.subscribe(List(topic).asJava)

  def subscribeConsumerLongByteArrayToTopic(topic: String): Unit =
    consumerLongByteArray.subscribe(List(topic).asJava)

  def getDefaultConsumerStringByteArray : Consumer[String, Array[Byte]] =
    consumerStringByteArray

  def getDefaultConsumerLongByteArray : Consumer[Long, Array[Byte]] =
    consumerLongByteArray

  def createInputConsumer(topic: String) : Consumer[String, Array[Byte]] = {

    val consumer = createConsumerStringByteArray()
    consumer.subscribe(List(topic).asJavaCollection)
    consumer.listTopics()
    consumer
  }

}

