import java.util.Properties
import java.util.concurrent.TimeUnit

import QueryTwo.{env, executeTumbling}
import flink_operators._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import utils.ranking.UserScore
import utils.Parser
import utils.flink.CommentsAvroDeserializationSchema


object QueryThree {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", utils.Configuration.BOOTSTRAP_SERVERS)
  properties.setProperty("zookeeper.connect", utils.Configuration.ZOOKEEPER_SERVERS)
  properties.setProperty("group.id", utils.Configuration.CONSUMER_GROUP_ID)



  val friendshipData = env.readTextFile("dataset/friendships.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0) ))

  val commentsData = env.readTextFile("dataset/comments.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ))

  val postsData = env.readTextFile("dataset/posts.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) ))

  /**
    * Executes the query using tumbling windows
    * @param outputPath file input path directory
    */
  def executeTumbling( outputPath : String) : Unit = {

    val hourlyResults = postsData.union(commentsData, friendshipData)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1) )) //, Time.minutes(30)))
      .aggregate(new UserScoreAggregator, new PartialRanker)
      .process(new GlobalRanker)

    hourlyResults.writeAsText("results/q3-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24) ))//, Time.hours(1) ))
      .process(new IncrementalRankMerger)

    dailyResults.writeAsText("results/q3-daily")

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7) ))//,Time.days(1)))
      .process(new IncrementalRankMerger)

    weeklyResults.writeAsText("results/q3-weekly")
  }

  /**
    * Executes the query using sliding windows
    * @param outputPath
    */
  def executeSliding( outputPath : String) : Unit = {
    val keyedUnitedStream =
      postsData.union(commentsData, friendshipData)
        .keyBy(_._1)

    val hourlyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(4)
      .process(new GlobalRanker)
      .setParallelism(1)

    hourlyResults.writeAsText(outputPath + "results/q2-hourly-sliding")

    val dailyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)

    dailyResults.writeAsText(outputPath + "results/q2-daily-sliding")

    val weeklyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)

    weeklyResults.writeAsText(outputPath + "results/q2-weekly-sliding")
  }

  def main(args: Array[String]) : Unit = {

    val params : ParameterTool = ParameterTool.fromArgs(args)

    val outputPath = params.getRequired("output")

    executeTumbling(outputPath)
    //executeSliding(outputPath)


    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }




}
