package utils.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization._
import utils.Configuration
import scala.collection.JavaConverters._


object ConsumerManager {

  val consumer: Consumer[String, Array[Byte]] = createConsumer()

  def createConsumer(): Consumer[String, Array[Byte]] = {

    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new ByteArrayDeserializer().getClass.getName)

    new KafkaConsumer[String, Array[Byte]](props)
  }

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeToTopic(topic: String): Unit =
    consumer.subscribe(List(topic).asJava)


  def getDefaultConsumer : Consumer[String, Array[Byte]] =
    this.consumer
}

