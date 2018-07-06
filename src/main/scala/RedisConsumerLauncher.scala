
import utils.Configuration
import utils.kafka.RedisResultsConsumer

/**
  * Lancher for consumer processes to read results published on the output topics of Kafka
  * and than send them to Redis storage.
  */
object RedisConsumerLauncher {

  def  launch(): Unit = {
    val TOPICS: List[String] = Configuration.OUTPUT_TOPICS

    val CONSUMERS_NUM: Int = TOPICS.length

    for (i <- 0 to CONSUMERS_NUM) {
      val c: Thread = new Thread{ new RedisResultsConsumer(i, TOPICS(i)).run()}
      c.start()
    }
  }

  def main(args: Array[String]): Unit = {
    launch()
  }
}
