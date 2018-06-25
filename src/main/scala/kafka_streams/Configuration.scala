package kafka_streams

object Configuration {

  val BOOTSTRAP_SERVERS: String = "localhost:9092"
//  val BOOTSTRAP_SERVERS: String = "localhost:9092,localhost:9093,localhost:9094"
  val ZOOKEEPER_SERVERS: String = "localhost:2181"
//  val ZOOKEEPER_SERVERS: String = "localhost:2181,localost:2888"

  val INPUT_TOPIC: String = "streams-app-input"
  val OUTPUT_TOPIC: String = "streams-app-output"

  val CONSUMER_GROUP_ID: String = "app-consumer1"
  val NUM_CONSUMERS: Int = 1

  val PRODUCER_ID : String = "app-producer1"

  val FRIENDSHIP_SCHEMA: String = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"friendship_record\"," +
    "\"fields\":[" +
    "  { \"name\":\"ts\", \"type\":\"long\" }," +
    "  { \"name\":\"user_id1\", \"type\":\"long\" }," +
    "  { \"name\":\"user_id2\", \"type\":\"long\" }" +
    "]}"

}
