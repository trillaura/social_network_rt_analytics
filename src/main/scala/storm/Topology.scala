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

    val config = new Config
    config.setNumWorkers(4)
    config.setMessageTimeoutSecs(5)

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

    /*
      HOURLY STATISTICS
     */

    builder.setBolt("hourlyCount", new WindowCountBolt().withSlidingWindow(
      Bolt.Config.hourlyCountWindowSize, 30 * 60 * 1000))
      .setNumTasks(1)
      .allGrouping("metronome", "sMetronome")
      .fieldsGrouping("filter", new Fields("post_commented"))

    builder.setBolt("hourlyPartialRank", new PartialRank)
      .setNumTasks(3)
      .fieldsGrouping("hourlyCount", new Fields("post_commented"))

    builder.setBolt("hourlyGlobalRank", new GlobalRank)
      .setNumTasks(3)
      .shuffleGrouping("hourlyPartialRank")

    /*
      DAILY STATISTICS
     */

    builder.setBolt("dailyCount", new WindowCountBolt().withSlidingWindow(
      Bolt.Config.dailyCountWindowSize, 60 * 60 * 1000))
      .setNumTasks(13)
      .allGrouping("metronome", "sMetronome")
      .fieldsGrouping("hourlyCount", new Fields("post_commented"))

    builder.setBolt("dailyPartialRank", new PartialRank)
      .setNumTasks(3)
      .fieldsGrouping("dailyCount", new Fields("post_commented"))

    builder.setBolt("dailyGlobalRank", new GlobalRank)
      .setNumTasks(1)
      .shuffleGrouping("dailyPartialRank")

    /*
      WEEKLY STATISTICS
     */

    builder.setBolt("weeklyCount", new WindowCountBolt().withSlidingWindow(
      Bolt.Config.weeklyCountWindowSize, 24 * 60 * 60 * 1000))
      .setNumTasks(13)
      .allGrouping("metronome", "sMetronome")
      .fieldsGrouping("dailyCount", new Fields("post_commented"))

    builder.setBolt("weeklyPartialRank", new PartialRank)
      .setNumTasks(3)
      .fieldsGrouping("weeklyCount", new Fields("post_commented"))

    builder.setBolt("weeklyGlobalRank", new GlobalRank)
      .setNumTasks(1)
      .shuffleGrouping("weeklyPartialRank")



    /*
      Collect the output
     */
    builder.setBolt("printer", new CollectorBolt())
      .setNumTasks(1)
      .shuffleGrouping("hourlyGlobalRank")
    //      .shuffleGrouping("dailyGlobalRank")
    //      .shuffleGrouping("weeklyGlobalRank")


    /*
      Create topology and submit it
     */

    val cluster = new LocalCluster
    cluster.submitTopology("query2", config, builder.createTopology)
    //    Thread.sleep(3600 * 1000)
    //    cluster.killTopology("query2")


    // cluster
    //        StormSubmitter.submitTopology(args(0), conf, stormTopology)

  }

}
