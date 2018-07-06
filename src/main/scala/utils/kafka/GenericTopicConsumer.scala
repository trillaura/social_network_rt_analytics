package utils.kafka

import org.apache.kafka.clients.consumer.Consumer
import scala.collection.JavaConverters._

object GenericTopicConsumer {

  var consumerStringByteArray: Consumer[String, Array[Byte]] = _
  var consumerLongByteArray: Consumer[Long, Array[Byte]] = _

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("\tUsage <key-type> <value-type> <topic>")
      return
    }

    val key_type = args(1)
    val value_type = args(2)
    val topic = args(3)

    if (key_type.toLowerCase.equals("string") && value_type.toLowerCase.equals("bytearray")) {
      consumerStringByteArray = new ConsumerManager().createInputConsumer(topic)
      consumerStringByteArray.subscribe(List(topic).asJavaCollection)

      val records = consumerStringByteArray.poll(1000)

      records.asScala.foreach(
        r => println(KafkaAvroParser.fromByteArrayToRecord(r.value, topic))
      )

    } else if (key_type.toLowerCase.equals("long") && value_type.toLowerCase.equals("bytearray")) {
      consumerLongByteArray = new ConsumerManager().createOutputConsumer(topic)
      consumerLongByteArray.subscribe(List(topic).asJavaCollection)

      val records = consumerLongByteArray.poll(1000)

      records.asScala.foreach(
        r => println(KafkaAvroParser.fromByteArrayToRecord(r.value, topic))
      )

    } else {
      println("\tNot managed yet")
    }
  }
}
