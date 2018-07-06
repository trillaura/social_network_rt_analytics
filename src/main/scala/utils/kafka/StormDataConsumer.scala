package utils.kafka

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords}
import utils.Configuration

/**
  * This class implements Kafka consumer as Storm Spout to get
  * data from input topics and process them in the topology.
  *
  * @param i consumer id
  * @param t topic name
  */
class StormDataConsumer(i : Int, t: String) {

  private val topic: String = t
  private val consumer: Consumer[String, Array[Byte]] = new ConsumerManager().createInputConsumer(topic)
  private val id: Int = i
  private var running: Boolean = true

  def consume() : ConsumerRecords[String, Array[Byte]] = {
    if (Configuration.DEBUG) { println("Consumer " + id + " running...")}
    Thread.sleep(1000)
    consumer.poll(1000)
  }

  def close() : Unit =
    consumer.close()

}
