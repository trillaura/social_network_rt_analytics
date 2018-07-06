package utils.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization._
import utils.Configuration

import scala.collection.JavaConverters._

/**
  * Object to get default propertie
  */
class ConsumerManager {

  private def getCommonProperties : Properties = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props
  }

  private def createConsumerStringByteArray(): Consumer[String, Array[Byte]] = {

    val props = getCommonProperties

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[String, Array[Byte]](props)
  }

  private def createConsumerLongByteArray(): Consumer[Long, Array[Byte]] = {

    val props = getCommonProperties

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new LongDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[Long, Array[Byte]](props)
  }

  private def listConsumerStringByteArrayTopics(consumer: Consumer[String, Array[Byte]]): Unit = {

    val topics: util.Map[String, util.List[PartitionInfo]] = consumer.listTopics()
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

  private def listConsumerLongByteArrayTopics(consumer: Consumer[Long, Array[Byte]]): Unit = {

    val topics: util.Map[String, util.List[PartitionInfo]] = consumer.listTopics()
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
  private def subscribeConsumerStringByteArrayToTopic(consumer: Consumer[String, Array[Byte]], topic: String): Unit =
    consumer.subscribe(List(topic).asJava)

  private def subscribeConsumerLongByteArrayToTopic(consumer: Consumer[Long, Array[Byte]], topic: String): Unit =
    consumer.subscribe(List(topic).asJava)

  def createInputConsumer(topic: String) : Consumer[String, Array[Byte]] = {

    val consumer = createConsumerStringByteArray()
    subscribeConsumerStringByteArrayToTopic(consumer, topic)
    listConsumerStringByteArrayTopics(consumer)
    consumer
  }

  def createOutputConsumer(topic: String) : Consumer[Long, Array[Byte]] = {
    val consumer = createConsumerLongByteArray()
    subscribeConsumerLongByteArrayToTopic(consumer, topic)
    listConsumerLongByteArrayTopics(consumer)
    consumer
  }
}

