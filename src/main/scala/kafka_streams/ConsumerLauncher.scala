package kafka_streams

import java.util
import java.util.Properties

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer, StringDeserializer}

import scala.collection.JavaConverters._

object ConsumerLauncher {

  var id: Int = -1

  var consumer: Consumer[String, String] = createConsumer()
  var consumerAvro: Consumer[String, Array[Byte]] = createAvroConsumer()

  var consumerWordCount : Consumer[String, Long] = createConsumerWordCount()

  def createConsumer(): Consumer[String, String] = {

    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)

    new KafkaConsumer[String, String](props)
  }

  def  createAvroConsumer(): Consumer[String, Array[Byte]] = {

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

  def createConsumerWordCount(): Consumer[String, Long] = {

    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      Configuration.BOOTSTRAP_SERVERS)

    props.put(ConsumerConfig.GROUP_ID_CONFIG,
      Configuration.CONSUMER_GROUP_ID)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      new StringDeserializer().getClass.getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      new LongDeserializer().getClass.getName)

    new KafkaConsumer[String, Long](props)
  }

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeToTopic(): Unit = {
    consumer.subscribe(List(Configuration.OUTPUT_TOPIC).asJava)
  }
  def subscribeToTopicAvro(): Unit = {
    consumerAvro.subscribe(List(Configuration.INPUT_TOPIC).asJava)
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

  def consumeAvro(): Unit = {
    subscribeToTopicAvro()

    var running: Boolean = true
    println("Avro Consumer " + id + " running...")

    while (running) {

      Thread.sleep(1000)

      val records =
        consumerAvro.poll(1000)

      records.asScala.foreach(avroRecord => {
        val parser: Schema.Parser = new Schema.Parser()
        val schema: Schema = parser.parse(Configuration.FRIENDSHIP_SCHEMA)
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

        val record: GenericRecord = recordInjection.invert(avroRecord.value()).get

        println("ts= " + record.get("ts")
          + ", user_id1= " + record.get("user_id1")
          + ", user_id2=" + record.get("user_id2"))
      })
    }
    consumerAvro.close()
  }

  def consume(): Unit = {

    subscribeToTopic()

    var running: Boolean = true
    println("Consumer " + id + " running...")

    while (running) {

      Thread.sleep(1000)
      val records =
        consumer.poll(1000)

      for (record <- records.asScala )
        println("[" + id + "] Consuming record:" +
        " (key=" + record.key() + ", " +
        "val=" + record.value() + ")")

    }
    consumer.close()
  }

  def main(args: Array[String]): Unit = {
    println("=== Listing topics === ")
    listTopics()
    println("=== === === === === === ")

    KafkaManager.createTopic(Configuration.OUTPUT_TOPIC, 1, 1: Short)

    for (i <- 0 until Configuration.NUM_CONSUMERS-1) {
      id = i
      val c: Thread = new Thread{
        override def run(): Unit ={
          consume()
        }
      }
      c.start()
    }
  }
}
