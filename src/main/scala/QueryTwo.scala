import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import utils.Parser
import utils.flink.CommentsAvroDeserializationSchema
import java.util.Properties

import flink_operators.{GlobalRanker, PartialRanker, SimpleScoreAggregator}
import utils.ranking._


/**
  * (120260221010,20)
  * (34361155726,20)
  * (103080479989,20)
  * (85900499913,19)
  * (128849726908,19)
  * (51540124111,19)
  * (120260181226,19)
  * (60129962143,19)
  * (120260171244,18)
  * (94490197116,18)
  */
object QueryTwo {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val data: DataStream[String] = env.readTextFile("dataset/comments.dat")


  val properties = new Properties()
  properties.setProperty("bootstrap.servers", utils.Configuration.BOOTSTRAP_SERVERS)
  properties.setProperty("zookeeper.connect", utils.Configuration.ZOOKEEPER_SERVERS)
  properties.setProperty("group.id", utils.Configuration.CONSUMER_GROUP_ID)

  private val stream = env
    .addSource(new FlinkKafkaConsumer011(utils.Configuration.COMMENTS_INPUT_TOPIC, new CommentsAvroDeserializationSchema, properties))



  def executeParallel() : Unit = {
    val hourlyResults = data
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps( _.timestamp.getMillis )
      .map(postComment => (postComment.parentID, SimpleScore(1)))
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(30)))
      .aggregate(new SimpleScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)



    hourlyResults.writeAsText("results/q2-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1) ))
      .reduce(_ mergeRank _)
      /*.keyBy(_.timestamp)
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .reduce(new RankingResultsReducer, new PartialRankingMerger)
      .setParallelism(2)
      .process(new MergeRank) */



    dailyResults.writeAsText("results/q2-daily")


    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
      .reduce(_ mergeRank _)

    weeklyResults.writeAsText("results/q2-weekly")

  }

  def main(args: Array[String]) : Unit = {

    executeParallel()

    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }
}