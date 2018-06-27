package kafka_streams

import java.util
import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer, StringDeserializer}
import utils.KafkaAvroParser

import scala.collection.JavaConverters._

object ConsumerLauncher {

  var consumerAvro: Consumer[String, Array[Byte]] = createAvroConsumer()

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

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeToTopicsAvro(topics: List[String]): Unit = {
    consumerAvro.subscribe(topics.asJava)
  }

  def listTopics(): Unit = {

    val topics: util.Map[String, util.List[PartitionInfo]] = consumerAvro.listTopics()
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

  def consumeAvroFriendships(id: Int): Unit = {

    var running: Boolean = true
    println("Avro Consumer " + id + " running...")

    while (running) {

      Thread.sleep(1000)

      val records =
        consumerAvro.poll(1000)

      records.asScala.foreach(avroRecord => {
        try {
          val record: GenericRecord =
            KafkaAvroParser.fromByteArrayToFriendshipRecord(avroRecord.value)

          println("ts = " + record.get("ts")
            + ", user_id1 = " + record.get("user_id1")
            + ", user_id2 =" + record.get("user_id2"))
        } catch {
          case _:Throwable => println("err")
        }
      })
    }
    consumerAvro.close()
  }

  def consumeAvroComments(id: Int): Unit = {

    var running: Boolean = true
    println("Avro Consumer " + id + " running...")

    while (running) {

      Thread.sleep(1000)

      val records =
        consumerAvro.poll(1000)

      records.asScala.foreach(avroRecord => {
        try {
          val record: GenericRecord =
            KafkaAvroParser.fromByteArrayToCommentRecord(avroRecord.value)

          println("ts = " + record.get("ts")
            + ", comment_id = " + record.get("comment_id")
            + ", user_id =" + record.get("user_id")
            + ", comment =" + record.get("comment")
            + ", user =" + record.get("user")
            + ", comment_replied =" + record.get("comment_replied")
            + ", post_commented =" + record.get("post_commented")
          )
        } catch {
          case _:Throwable => println("err")
        }
      })
    }
    consumerAvro.close()
  }

  def consumeAvroPosts(id: Int): Unit = {

    var running: Boolean = true
    println("Avro Consumer " + id + " running...")

    while (running) {

      Thread.sleep(1000)

      val records =
        consumerAvro.poll(1000)

      records.asScala.foreach(avroRecord => {
        try {
          val record: GenericRecord =
            KafkaAvroParser.fromByteArrayToPostRecord(avroRecord.value)

          println("ts = " + record.get("ts")
            + ", post_id = " + record.get("post_id")
            + ", user_id =" + record.get("user_id")
            + ", post =" + record.get("post")
            + ", user =" + record.get("user")
          )
        } catch {
          case _:Throwable => println("err")
        }
      })
    }
    consumerAvro.close()
  }

  def main(args: Array[String]): Unit = {

    val topics =  List(Configuration.FRIENDS_INPUT_TOPIC, Configuration.COMMENTS_INPUT_TOPIC, Configuration.POSTS_INPUT_TOPIC)

    for (t <- topics) {
      KafkaManager.createTopic(t, 1, 1: Short)
    }

    subscribeToTopicsAvro(topics)

//    consumeAvroFriendships()
//    consumeAvroComments()
//    consumeAvroPosts()

    for (i <- 0 until Configuration.NUM_CONSUMERS-1) {
      if (i == 0) {
        new Thread {
          override def run(): Unit = {
            consumeAvroFriendships(i)
          }
        }.start()
      } else if (i == 1) {
        new Thread {
          override def run(): Unit = {
            consumeAvroComments(i)
          }
        }.start()
      } else {
        new Thread {
          override def run(): Unit = {
            consumeAvroPosts(i)
          }
        }.start()
      }
    }
  }
}
