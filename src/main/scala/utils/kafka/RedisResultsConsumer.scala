package utils.kafka

import org.apache.kafka.clients.consumer.Consumer
import utils.Configuration
import utils.redis.RedisManager

import scala.collection.JavaConverters._

/**
  * This class implements a consumer of the results published on
  * output topics by processing subsystem.
  *
  * @param i consumer identifier
  * @param t topic name
  */
class RedisResultsConsumer(i: Int, t: String) extends Runnable {

  private val topic: String = t
  private val consumer: Consumer[Long, Array[Byte]] = new ConsumerManager().createOutputConsumer(topic)
  private val id: Int = i
  private var running: Boolean = true

//  // To consume data, we first need to subscribe to the topics of interest
//  def subscribeToTopic(): Unit = {
//    consumer.subscribe(List(this.topic).asJava)
//  }

  def run(): Unit = {

//    subscribeToTopic()

    if (Configuration.DEBUG) { println("Consumer "+ id +" running...") }

    while (running) {

      Thread.sleep(1000)

      val records =
        consumer.poll(1000)

      RedisManager.writeAsyncInSortedSet(records, topic)
    }
    consumer.close()
  }
}
