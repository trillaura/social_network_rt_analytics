package utils.kafka

import org.apache.kafka.clients.consumer.Consumer
import utils.redis.RedisManager

import scala.collection.JavaConverters._

class RedisResultsConsumer(i: Int, t: String) extends Runnable {

  private val consumer: Consumer[Long, Array[Byte]] = ConsumerManager.getDefaultConsumerLongByteArray
  private val topic: String = t
  private val id: Int = i
  private var running: Boolean = true

  // To consume data, we first need to subscribe to the topics of interest
  def subscribeToTopic(): Unit = {
    consumer.subscribe(List(this.topic).asJava)
  }

  def run(): Unit = {

    subscribeToTopic()

    println("Consumer "+ id +" running...")

    while (running) {

      Thread.sleep(1000)

      val records =
        consumer.poll(1000)

      RedisManager.writeAsyncInSortedSet(records, topic)
    }
    consumer.close()
  }
}
