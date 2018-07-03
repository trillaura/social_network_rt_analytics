
import utils.Configuration
import utils.kafka.RedisResultsConsumer

object RedisConsumerLauncher {

  def main(args: Array[String]): Unit = {

    val TOPICS: List[String] = Configuration.OUTPUT_TOPICS

    val CONSUMERS_NUM: Int = TOPICS.length

    for (i <- 0 to CONSUMERS_NUM) {
      val c: Thread = new Thread{ new RedisResultsConsumer(i, TOPICS(i)).run()}
      c.start()
    }
  }
}
