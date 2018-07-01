
import utils.Configuration
import utils.kafka.{KafkaManager, ProducerLauncher}

object DataProducer {

  def main(args: Array[String]): Unit = {

    val FREQUENCY : Int  = 100000

    for (t <- Configuration.INPUT_TOPICS) { KafkaManager.createTopic(t, 1, 1: Short) }

    for (t <- Configuration.OUTPUT_TOPICS) { KafkaManager.createTopic(t, 1, 1: Short) }

    val p1: Thread = new Thread{new ProducerLauncher(Configuration.FRIENDS_INPUT_TOPIC, FREQUENCY)}
    val p2: Thread = new Thread{new ProducerLauncher(Configuration.COMMENTS_INPUT_TOPIC, FREQUENCY)}
    val p3: Thread = new Thread{new ProducerLauncher(Configuration.POSTS_INPUT_TOPIC, FREQUENCY)}
    p1.start()
    p2.start()
    p3.start()
  }
}
