package storm

import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt.Duration
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import storm.Bolt._
import storm.Spout.SimpleSpout
import org.apache.storm.{Config, LocalCluster, StormSubmitter}


object Topology {

  def main(args: Array[String]): Unit = {

    val builder: TopologyBuilder = new TopologyBuilder

    builder.setSpout("spout", new SimpleSpout)

    builder.setBolt("parser", new ParseLine())
      .setNumTasks(3)
      .shuffleGrouping("spout")

    builder.setBolt("filter", new Filtering)
      .setNumTasks(3)
      .shuffleGrouping("parser")

    builder.setBolt("metronome", new Metronome)
        .setNumTasks(1)
        .shuffleGrouping("filter")

    builder.setBolt("hourlyCount", new WindowCountBolt().withSlidingWindow(Duration.minutes(60), Duration.minutes(5)))
        .setNumTasks(3)
        .fieldsGrouping("filter", new Fields("post_commented"))
        .allGrouping("metronome")

    builder.setBolt("partialRank", new PartialRank)
        .setNumTasks(3)
        .fieldsGrouping("hourlyCount", new Fields("post_commented"))

    builder.setBolt("globalRank", new GlobalRank)
        .setNumTasks(1)
        .allGrouping("partialRank")
    /*
      Collect the output
     */
    builder.setBolt("printer", new CollectorBolt())
      .setNumTasks(1)
      .shuffleGrouping("globalRank")
//      .shuffleGrouping("dailyGlobalRank")
//      .shuffleGrouping("dailyGlobalRank")
//      .shuffleGrouping("weeklyGlobalRank")


    /*
      Create topology and submit it
     */

    val stormTopology: StormTopology = builder.createTopology()
    /* Create configurations */
    val conf = new Config
    conf.setDebug(false)
    /* number of workers to create for current topology */
    conf.setNumWorkers(3)

    /* Update numWorkers using command-line received parameters */
    if (args.length == 2)
      if (args(1) != null) {
        val numWorkers = args(1).toInt
        conf.setNumWorkers(numWorkers)
        System.out.println("Number of workers to generate for current topology set to: " + numWorkers)
      }


    // local
    val cluster = new LocalCluster()
    cluster.submitTopology("debs", conf, stormTopology)
    Utils.sleep(100000)
    cluster.killTopology("debs")
    cluster.shutdown()

    // cluster
    //        StormSubmitter.submitTopology(args(0), conf, stormTopology)

  }

}
