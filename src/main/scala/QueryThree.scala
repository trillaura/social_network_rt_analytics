import java.util.concurrent.TimeUnit

import QueryOne.{env, properties}
import flink_operators.{GlobalRanker, IncrementalRankMerger, PartialRanker, UserScoreAggregator}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import utils.flink.{FriedshipAvroDeserializationSchema, ResultAvroSerializationSchemaRanking}
import utils.ranking.UserScore
import utils.{Configuration, Parser}


object QueryThree {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val friendshipData: DataStream[(Long, UserScore)] = env.addSource(
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
    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) ))

//  val friendshipData = env.readTextFile("dataset/friendships.dat")
//    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
//    .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0) ))

//  val commentsData = env.readTextFile("dataset/comments.dat")
//    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
//    .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ))

//  val postsData = env.readTextFile("dataset/posts.dat")
//    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
//    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) ))

  def main(args: Array[String]) : Unit = {

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
      //.reduce(_ mergeRank _)

    dailyResults.writeAsText("results/q3-daily")

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7) ))//,Time.days(1)))
      .process(new IncrementalRankMerger)
      //.reduce(_ mergeRank _)

    weeklyResults.writeAsText("results/q3-weekly")

    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")

    /*
   Adding sink: Write on Kafka topic
*/

    hourlyResults.addSink(
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
    )

  }




}
