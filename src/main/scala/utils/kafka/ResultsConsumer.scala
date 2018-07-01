package utils.kafka

import java.util
import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer}
import utils.Configuration

import scala.collection.JavaConverters._

class ResultsConsumer(cons_id: Int, t: String) extends Runnable {

  private val consumer: Consumer[Long, Array[Byte]] = createConsumer()
  private val topic: String = t
  private val id: Int = cons_id
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

    for (t <- Configuration.OUTPUT_TOPICS) {
      KafkaManager.createTopic(t, 1, 1: Short)
    }

    subscribeToTopic()

    while (running) {

      Thread.sleep(1000)

      val records =
        consumer.poll(1000)

      records.asScala.foreach(r => {
        try {
          val record: GenericRecord =
            KafkaAvroParser.fromByteArrayToResultRecord(r.value, topic)

          // TODO write on Redis sorted set keyed by stat name sorted by timestamp
        } catch {
          case _:Throwable => println("err")
        }
      })
    }
    consumer.close()
  }
}
