package utils.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer, StringDeserializer}
import utils.Configuration
import utils.redis.RedisManager

import scala.collection.JavaConverters._

class RedisResultsConsumer(i: Int, t: String) extends Runnable {

  private val consumer: Consumer[Long, Array[Byte]] = createConsumer()
  private val topic: String = t
  private val id: Int = i
  private var running: Boolean = true



  def createConsumer(): Consumer[Long, Array[Byte]] = {

    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new LongDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[Long, Array[Byte]](props)
  }

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeToTopic(): Unit = {
    consumer.subscribe(List(this.topic).asJava)
  }

  def listTopics(): Unit = {

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

  def run(): Unit = {

    subscribeToTopic()

    while (running) {

      Thread.sleep(1000)

      val records =
        consumer.poll(1000)

      RedisManager.writeAsyncInSortedSet(records, topic)
    }
    consumer.close()
  }
}
