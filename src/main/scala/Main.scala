import kafka_streams.Query1
import storm.Topology
import utils.Configuration

object Main {

  def main(args: Array[String]) : Unit = {

    if (args.length == 8) {
      if (args(1).toLowerCase.equals("kafka")) {        // kafka subsystem

        val broker0 = args(2)
        val broker1 = args(3)
        val broker2 = args(4)
        val zookeeper0 = args(5)
        val zookeeper1 = args(6)
        val zookeeper2 = args(7)

        Configuration.BOOTSTRAP_SERVERS = broker0 + "," + broker1 + "," + broker2
        Configuration.ZOOKEEPER_SERVERS = zookeeper0 + "," + zookeeper1 + "," + zookeeper2

        Query1.execute()

      } else if (args(1).toLowerCase.equals("flink")) { // flink subsystem

        val broker0 = args(2)
        val broker1 = args(3)
        val broker2 = args(4)
        val zookeeper0 = args(5)
        val zookeeper1 = args(6)
        val zookeeper2 = args(7)

        Configuration.BOOTSTRAP_SERVERS = broker0 + "," + broker1 + "," + broker2
        Configuration.ZOOKEEPER_SERVERS = zookeeper0 + "," + zookeeper1 + "," + zookeeper2

        QueryOne.main(new Array[String](1))
        QueryTwo.main(new Array[String](1))
        QueryThree.main(new Array[String](1))

      } else if (args(1).toLowerCase.equals("storm")) { //storm subsystem

        val broker0 = args(2)
        val broker1 = args(3)
        val broker2 = args(4)
        val zookeeper0 = args(5)
        val zookeeper1 = args(6)
        val zookeeper2 = args(7)

        Configuration.BOOTSTRAP_SERVERS = broker0 + "," + broker1 + "," + broker2
        Configuration.ZOOKEEPER_SERVERS = zookeeper0 + "," + zookeeper1 + "," + zookeeper2

        Topology.main(new Array[String](1))

      } else { // redis consumer

        val broker0 = args(1)
        val broker1 = args(2)
        val broker2 = args(3)
        val zookeeper0 = args(4)
        val zookeeper1 = args(5)
        val zookeeper2 = args(6)
        val redis = args(7).split(':')

        Configuration.BOOTSTRAP_SERVERS = broker0 + "," + broker1 + "," + broker2
        Configuration.ZOOKEEPER_SERVERS = zookeeper0 + "," + zookeeper1 + "," + zookeeper2

        Configuration.REDIS_HOST = redis(0)
        Configuration.REDIS_PORT = redis(1).toInt

        RedisConsumerLauncher.launch()
      }
    } else if (args.length == 7) { // publisher

      val broker0 = args(1)
      val broker1 = args(2)
      val broker2 = args(3)
      val zookeeper0 = args(4)
      val zookeeper1 = args(5)
      val zookeeper2 = args(6)

      Configuration.BOOTSTRAP_SERVERS = broker0 + "," + broker1 + "," + broker2
      Configuration.ZOOKEEPER_SERVERS = zookeeper0 + "," + zookeeper1 + "," + zookeeper2

      ProducerLauncher.launch()
    } else {
      println("\tUsage <subsystem[kafka/storm/flink]> <broker0> <broker1> <broker2> <zookeeper0> <zookeeper1> <zookeeper2>") // Queries
      println("\tUsage <broker0> <broker1> <broker2> <zookeeper0> <zookeeper1> <zookeeper2>") // ProducerLauncher
      println("\tUsage <broker0> <broker1> <broker2> <zookeeper0> <zookeeper1> <zookeeper2> <redis-server>") // RedisConsumerLauncher o RedisManager
    }
  }
}
