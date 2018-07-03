package utils.kafka

import java.util.Properties

import org.apache.kafka.clients.admin._
import utils.Configuration

import scala.collection.JavaConverters._


object KafkaManager {

  val config: Properties = new Properties
  config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)

  val sessionTimeoutMs: Int = 10 * 1000
  val connectionTimeoutMs: Int = 8 * 1000

  val admin: AdminClient = AdminClient.create(config)

  val zkServer: String = Configuration.ZOOKEEPER_SERVERS

  def listTopics(): ListTopicsResult = {
    val list = admin.listTopics(new ListTopicsOptions().timeoutMs(1000).listInternal(true))
    list.names().get().asScala.foreach(t => println(t))
    list
  }

  def topicExists(topic: String): Boolean = {
    val list = listTopics()
    for (t <- list.names().get().asScala) {
      if (t.equals(topic)) { return true }
    }
    false
  }

  def createTopic(topic: String, partitions: Int, replication: Short): Unit = {

//    val topicConfig = new Properties() // add per-topic configurations settings here

    if (!topicExists(topic)) {

      val newTopic = new NewTopic(topic, partitions, replication)
      newTopic.configs(Map[String,String]().asJava)

      admin.createTopics(List(newTopic).asJavaCollection)
      //    ret.all().get() // Also fails
    }
//    admin.close()
  }

}
