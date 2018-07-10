import java.util.concurrent.TimeUnit

import QueryOne.properties
import QueryThreeMetric.{env, executeSliding, executeTumbling}
import flink_operators.{GlobalRanker, IncrementalRankMerger, PartialRanker, UserScoreAggregator}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import utils.flink.{FriedshipAvroDeserializationSchema, ResultAvroSerializationSchemaRanking}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import utils.ranking.UserScore
import utils.Configuration
import utils.Parser


object QueryThree {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setLatencyTrackingInterval(5L)
  env.setParallelism(1)

  /*
   * Data From Kafka
   */

  /*val friendshipData: DataStream[(Long, UserScore)] = env.addSource(
    new FlinkKafkaConsumer011(Configuration.FRIENDS_INPUT_TOPIC, new FriedshipAvroDeserializationSchema, properties))
    .asInstanceOf[DataStream[String]]
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0) ))

  val commentsData : DataStream[(Long, UserScore)] = env.addSource(
      new FlinkKafkaConsumer011(Configuration.COMMENTS_INPUT_TOPIC, new FriedshipAvroDeserializationSchema, properties))
    .asInstanceOf[DataStream[String]]
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ))

  val postsData: DataStream[(Long, UserScore)] = env.addSource(
      new FlinkKafkaConsumer011(Configuration.POSTS_INPUT_TOPIC, new FriedshipAvroDeserializationSchema, properties))
    .asInstanceOf[DataStream[String]]
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) )) */


  /**
    * Data from local file
    */

  //  val friendshipData = env.readTextFile("dataset/friendships.dat")
  //    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
  //    .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0) ))

  //  val commentsData = env.readTextFile("dataset/comments.dat")
  //    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
  //    .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ))

  //  val postsData = env.readTextFile("dataset/posts.dat")
  //    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
  //    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) ))

  /**
    * Executes the query using tumbling windows
    */
  def executeTumbling( friendshipData : DataStream[(Long, UserScore)],
                       commentsData: DataStream[(Long, UserScore)],
                       postsData: DataStream[(Long, UserScore)],
                       parserParallelism: Int, rankParallelism: Int) : Unit = {



    val hourlyResults = postsData.union(commentsData, friendshipData)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1) ))
      .aggregate(new UserScoreAggregator, new PartialRanker)
      .setParallelism(rankParallelism)
      .process(new GlobalRanker)
      .setParallelism(1)

    //hourlyResults.writeAsText("results/q3-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24) ))
      .process(new IncrementalRankMerger)

    //dailyResults.writeAsText("results/q3-daily")

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7) ))
      .process(new IncrementalRankMerger)

    //weeklyResults.writeAsText("results/q3-weekly")


    /**
      * Adding sink: Write on Kafka topic
      */
    /*hourlyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.POSTS_OUTPUT_TOPIC_H1,
        new ResultAvroSerializationSchemaRanking(Configuration.POSTS_OUTPUT_TOPIC_H1)
      )
    )

    dailyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.POSTS_OUTPUT_TOPIC_H24,
        new ResultAvroSerializationSchemaRanking(Configuration.POSTS_OUTPUT_TOPIC_H24)
      )
    )

    weeklyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.POSTS_OUTPUT_TOPIC_7D,
        new ResultAvroSerializationSchemaRanking(Configuration.POSTS_OUTPUT_TOPIC_7D))
    ) */
  }

  /**
    * Executes the query using sliding windows
    */
  def executeSliding(  friendshipData : DataStream[(Long, UserScore)],
                       commentsData: DataStream[(Long, UserScore)],
                       postsData: DataStream[(Long, UserScore)],
                       parserParallelism: Int, rankParallelism: Int) : Unit = {

    val keyedUnitedStream =
      postsData.union(commentsData, friendshipData)
        .keyBy(_._1)

    val hourlyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(rankParallelism * 2)
      .process(new GlobalRanker)
      .setParallelism(1)

    //hourlyResults.writeAsText(outputPath + "results/q2-hourly-sliding")

    val dailyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(rankParallelism * 2 - 1)
      .process(new GlobalRanker)
      .setParallelism(1)

    //dailyResults.writeAsText(outputPath + "results/q2-daily-sliding")

    val weeklyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
      .aggregate(new UserScoreAggregator,new PartialRanker)
      .setParallelism(rankParallelism)
      .process(new GlobalRanker)
      .setParallelism(1)

    //weeklyResults.writeAsText(outputPath + "results/q2-weekly-sliding")
  }

  def main(args: Array[String]) : Unit = {

    val params : ParameterTool = ParameterTool.fromArgs(args)

    val comments = params.get("comments",utils.Configuration.DATASET_COMMENTS)
    val posts = params.get("posts", utils.Configuration.DATASET_POSTS)
    val friendships = params.get("friendships", utils.Configuration.DATASET_FRIENDSHIPS)

    val parserParallelism = params.getInt("parser-parallelism", 2)
    val rankParallelism = params.getInt("rank-parallelism",2)

    val windowType = params.get("window", "tumbling")

    val friendshipData: DataStream[(Long, UserScore)] =
      env.readTextFile(friendships)
        .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
        .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0)))
        .setParallelism(parserParallelism)

    val commentsData : DataStream[(Long, UserScore)] =
      env.readTextFile(comments)
        .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
        .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1)))
        .setParallelism(parserParallelism)

    val postsData: DataStream[(Long, UserScore)] =
      env.readTextFile(posts)
        .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
        .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0)))
        .setParallelism(parserParallelism)

    if(windowType == "tumbling"){
      executeTumbling(friendshipData, commentsData, postsData,parserParallelism, rankParallelism)
    } else if(windowType == "sliding"){
      executeSliding(friendshipData, commentsData, postsData,parserParallelism, rankParallelism)
    }



    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")



  }




}
