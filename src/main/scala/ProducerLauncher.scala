
import utils.Configuration
import utils.kafka.{KafkaManager, DataProducer}

object ProducerLauncher {

  def launch() : Unit = {
    val FREQUENCY : Int  = 100000

    for (t <- Configuration.INPUT_TOPICS) { KafkaManager.createTopic(t, 1, 1: Short) }

    for (t <- Configuration.OUTPUT_TOPICS) { KafkaManager.createTopic(t, 1, 1: Short) }

//    val p1: Thread = new Thread{new DataProducer(Configuration.FRIENDS_INPUT_TOPIC, FREQUENCY).run()}
        val p2: Thread = new Thread{new DataProducer(Configuration.COMMENTS_INPUT_TOPIC, FREQUENCY).run()}
    //    val p3: Thread = new Thread{new DataProducer(Configuration.POSTS_INPUT_TOPIC, FREQUENCY).run()}
//    p1.start()
        p2.start()
    //    p3.start()
  }

  def main(args: Array[String]): Unit = {
    launch()
  }
}
