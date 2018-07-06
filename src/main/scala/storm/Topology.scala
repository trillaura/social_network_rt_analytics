package storm

import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.{Config, LocalCluster, StormSubmitter}
import storm.Bolt._
import storm.Spout.SimpleSpout


object Topology {

  def main(args: Array[String]): Unit = {

    val config = new Config
    config.setNumWorkers(3)
    config.setMessageTimeoutSecs(5)
    config.setDebug(false)

    val builder: TopologyBuilder = new TopologyBuilder

    builder.setSpout("spout", new SimpleSpout)

    builder.setBolt("parser", new ParseLine())
      .setNumTasks(3)
      .shuffleGrouping("spout")

    builder.setBolt("filter", new Filtering)
      .setNumTasks(3)
      .shuffleGrouping("parser")

    builder.setBolt("metronome", Metronome)
      .setNumTasks(1)
      .shuffleGrouping("filter")

    /*
      HOURLY STATISTICS
     */

    builder.setBolt("hourlyCount", new WindowCountBolt().withSlidingWindow(
      Bolt.Config.hourlyCountWindowSize, Bolt.Config.hourlyCountWindowSlide))
      .setNumTasks(3)
      .allGrouping("metronome", Metronome.S_METRONOME_HOURLY)
      .fieldsGrouping("filter", new Fields("post_commented"))

    builder.setBolt("hourlyPartialRank", new PartialRank)
      .setNumTasks(3)
      .fieldsGrouping("hourlyCount", new Fields("post_commented"))

    builder.setBolt("hourlyGlobalRank", new GlobalRank)
      .setNumTasks(1)
      .shuffleGrouping("hourlyPartialRank")

    /*
      DAILY STATISTICS
     */

    builder.setBolt("dailyCount", new WindowCountBolt().withSlidingWindow(
      Bolt.Config.dailyCountWindowSize, Bolt.Config.dailyCountWindowSlide))
      .setNumTasks(3)
      .allGrouping("metronome", Metronome.S_METRONOME_DAiLY)
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
      Bolt.Config.weeklyCountWindowSize, Bolt.Config.weeklyCountWindowSlide))
      .setNumTasks(3)
      .allGrouping("metronome", Metronome.S_METRONOME_WEEKLY)
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
      .shuffleGrouping("dailyGlobalRank")
      .shuffleGrouping("weeklyGlobalRank")


    /*
      Create topology and submit it
     */

    if (args.length == 2) {

      if (args(1) == "local") {
        val cluster = new LocalCluster
        cluster.submitTopology("query2", config, builder.createTopology)
      } else if (args(1) == "cluster")
        StormSubmitter.submitTopology(args(0), config, builder.createTopology())

    } else {
      val cluster = new LocalCluster
      cluster.submitTopology("query2", config, builder.createTopology)
    }


  }

}
