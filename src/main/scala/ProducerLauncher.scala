
import org.apache.flink.api.java.utils.ParameterTool
import utils.Configuration
import utils.kafka.{DataProducer, KafkaManager}

/**
  * Launcher for data producers parametrized with frequency of sending to Kafka
  * input topics.
  */
object ProducerLauncher {

  def launch(): Unit = {
    launch(Configuration.BOOTSTRAP_SERVERS, Configuration.ZOOKEEPER_SERVERS,"fcp")
  }

  def launch(boostrap : String,zookeeper: String,streamSelector: String) : Unit = {
    val FREQUENCY : Int  = 100000

    /* NOT WORKING ON CLOUD DEPLOYMENT. MANUALLY CREATE TOPICS OR WITH PROVIDED SCRIPT IN kubernetes/kafka/create_topics.sh */

    //val kafka = new KafkaManager(boostrap,zookeeper)
    //KafkaManager.clearAll()
    //kafka.clearAll()
    //for (t <- Configuration.INPUT_TOPICS) { kafka.createTopic(t, 1, 1: Short) }
    //for (t <- Configuration.OUTPUT_TOPICS) { kafka.createTopic(t, 1, 1: Short) }


    for(stream <- streamSelector) {
      stream match {
        case 'f' => new Thread{new DataProducer(Configuration.FRIENDS_INPUT_TOPIC, FREQUENCY, boostrap).run()}.start()
        case 'c' => new Thread{new DataProducer(Configuration.COMMENTS_INPUT_TOPIC, FREQUENCY, boostrap).run()}.start()
        case 'p' => new Thread{new DataProducer(Configuration.POSTS_INPUT_TOPIC, FREQUENCY, boostrap).run()}.start()
        case _ => println("No Stream specified")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val params : ParameterTool= ParameterTool.fromArgs(args)
    val bootstrapServers = params.get("bootstrap")
    val zookeeper = params.get("zookeeper", Configuration.ZOOKEEPER_SERVERS)
    val streamSelector = params.get("streams", "fcp")

    launch(bootstrapServers,zookeeper,streamSelector)
  }
}
