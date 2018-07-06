import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import utils.{Configuration, Parser}
import utils.flink.CommentsAvroDeserializationSchema
import java.util.Properties

import flink_operators.{GlobalRanker, IncrementalRankMerger, PartialRanker, SimpleScoreAggregator}
import org.apache.flink.api.java.utils.ParameterTool
import utils.flink.CommentsAvroDeserializationSchema
import utils.ranking._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import utils.{Configuration, Parser}
import utils.flink.{CommentsAvroDeserializationSchema, ResultAvroSerializationSchemaRanking}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import utils.Parser


/**
  * Computes the ranking of posts for given event-time window
  */
object QueryTwo {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)


  val properties = new Properties()
  properties.setProperty("bootstrap.servers", utils.Configuration.BOOTSTRAP_SERVERS)
  properties.setProperty("zookeeper.connect", utils.Configuration.ZOOKEEPER_SERVERS)
  properties.setProperty("group.id", utils.Configuration.CONSUMER_GROUP_ID)

  /* use this if data is coming from Kafka */
  private val stream = env
    .addSource(new FlinkKafkaConsumer011(utils.Configuration.COMMENTS_INPUT_TOPIC, new CommentsAvroDeserializationSchema, properties))


  /**
    * Executes the query using tumbling windows
    * @param inputPath file input path
    * @param outputPath file input path directory
    */
  def executeTumbling(inputPath: String, outputPath : String) : Unit = {

    val hourlyResults =  env.readTextFile(inputPath)
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps( _.timestamp.getMillis )
      .map(postComment => (postComment.parentID, SimpleScore(1)))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(new SimpleScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)


    hourlyResults.writeAsText(outputPath + "results/q2-hourly-p")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
      .process(new IncrementalRankMerger)

    dailyResults.writeAsText(outputPath + "results/q2-daily-p")

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
      .process(new IncrementalRankMerger)

    weeklyResults.writeAsText(outputPath + "results/q2-weekly-p")

  }

  /**
    * Executes the query using sliding windows
    * @param inputPath
    * @param outputPath
    */
  def executeSliding(inputPath: String, outputPath : String) : Unit = {

    /* main filtered data to use in different windows */
    val keyedFilteredData = env.readTextFile(inputPath)
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps( _.timestamp.getMillis )
      .map(postComment => (postComment.parentID, SimpleScore(1)))
      .keyBy(_._1)


    val hourlyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new SimpleScoreAggregator,new PartialRanker)
      .setParallelism(4)
      .process(new GlobalRanker)
      .setParallelism(1)

    hourlyResults.writeAsText(outputPath + "results/q2-hourly-sliding")

    val dailyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .aggregate(new SimpleScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)

    dailyResults.writeAsText(outputPath + "results/q2-daily-sliding")

    val weeklyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
      .aggregate(new SimpleScoreAggregator,new PartialRanker)
      .setParallelism(2)
      .process(new GlobalRanker)
      .setParallelism(1)

    /*
       Adding sink: Write on Kafka topic
    */

    hourlyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.COMMENTS_OUTPUT_TOPIC_H1,
        new ResultAvroSerializationSchemaRanking(Configuration.COMMENTS_OUTPUT_TOPIC_H1)
      )
    )

    dailyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.COMMENTS_OUTPUT_TOPIC_H24,
        new ResultAvroSerializationSchemaRanking(Configuration.COMMENTS_OUTPUT_TOPIC_H24)
      )
    )

    weeklyResults.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.COMMENTS_OUTPUT_TOPIC_7D,
        new ResultAvroSerializationSchemaRanking(Configuration.COMMENTS_OUTPUT_TOPIC_7D))
    )

  }



  def main(args: Array[String]) : Unit = {

    val params : ParameterTool = ParameterTool.fromArgs(args)

    val inputPath = params.getRequired("input")
    val outputPath = params.getRequired("output")


    executeTumbling(inputPath, outputPath)
    //executeSliding(inputPath, outputPath)


    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }
}



