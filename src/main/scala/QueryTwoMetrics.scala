import java.{lang, util}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.Properties

import QueryTwo.stream
import QueryTwoMetrics.toLatencyComment
import flink_operators.{GlobalRanker, IncrementalRankMerger, PartialRanker, SimpleScoreAggregator}
import model.{Comment, User}
import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import utils.ranking._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import utils.Configuration
import utils.flink.{CommentsAvroDeserializationSchema, ResultAvroSerializationSchemaRanking}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import utils.Parser
import utils.metrics.LatencyMeter

import scala.collection.mutable


/**
  * Computes the ranking of posts for given event-time window
  */
object QueryTwoMetrics {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val METRICS_SELECTOR = "metric"
  val RANKING_SELECTOR = "rank"

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
  def executeTumbling(inputPath: String, outputPath : String, parserParallelism: Int, rankParallelism: Int, writeResultsToFile: Boolean) : Unit = {

    val hourlyResults =  env.readTextFile(inputPath)
      .flatMap(new MetricFlatMap)
      .setParallelism(parserParallelism)
      .assignAscendingTimestamps( _.timestamp.getMillis )
      .setParallelism(parserParallelism)
      .map(postComment => (postComment.parentID, SimpleScore(1), postComment.latencyTimeStart))
      .setParallelism(parserParallelism)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(new SimpleScoreAggregatorMetric,new PartialRankerMetric)
      .setParallelism(rankParallelism)
      .process(new GlobalRankerMetric("Hourly"))
      .setParallelism(1)
      .split(new MetricOutputSelector)


    if(writeResultsToFile) {
      hourlyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-hourly-tumbling")
      hourlyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-hourly-tumbling")
    }

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
      .process(new IncrementalRankMergerMetric("Daily"))
      .split(new MetricOutputSelector)

    if(writeResultsToFile) {
      dailyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-daily-tumbling")
      dailyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-daily-tumbling")
    }

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
      .process(new IncrementalRankMergerMetric("Weekly"))
      .split(new MetricOutputSelector)

    if(writeResultsToFile) {
      weeklyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-weekly-tumbling")
      weeklyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-weekly-tumbling")
    }

  }

  /**
    * Executes the query using sliding windows
    * @param inputPath
    * @param outputPath
    */
  def executeSliding(inputPath: String, outputPath : String, parserParallelism: Int, rankParallelism: Int, writeResultsToFile: Boolean) : Unit = {

    /* main filtered data to use in different windows */
    val keyedFilteredData = env.readTextFile(inputPath)
      .flatMap(new MetricFlatMap)
      .setParallelism(parserParallelism)
      .assignAscendingTimestamps(_.timestamp.getMillis)
      .setParallelism(parserParallelism)
      .map(postComment => (postComment.parentID, SimpleScore(1), postComment.latencyTimeStart))
      .setParallelism(parserParallelism)
      .keyBy(_._1)


    val hourlyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new SimpleScoreAggregatorMetric, new PartialRankerMetric)
      .setParallelism(rankParallelism * 2)
      .process(new GlobalRankerMetric("Hourly"))
      .setParallelism(1)
      .split(new MetricOutputSelector)

    if(writeResultsToFile) {
      hourlyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-hourly-sliding")
      hourlyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-hourly-sliding")
    }

    val dailyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .aggregate(new SimpleScoreAggregatorMetric, new PartialRankerMetric)
      .setParallelism(rankParallelism * 2)
      .process(new GlobalRankerMetric("Daily"))
      .setParallelism(1)
      .split(new MetricOutputSelector)

    if(writeResultsToFile) {
      dailyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-daily-sliding")
      dailyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-daily-sliding")
    }

    val weeklyResults = keyedFilteredData
      .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
      .aggregate(new SimpleScoreAggregatorMetric, new PartialRankerMetric)
      .setParallelism(rankParallelism)
      .process(new GlobalRankerMetric("Weekly"))
      .setParallelism(1)
      .split(new MetricOutputSelector)

    if(writeResultsToFile) {
      weeklyResults.select(METRICS_SELECTOR).writeAsText(outputPath + "/metrics/q2-weekly-sliding")
      weeklyResults.select(RANKING_SELECTOR).writeAsText(outputPath + "/results/q2-weekly-sliding")
    }
  }



    def main(args: Array[String]) : Unit = {

    val params : ParameterTool = ParameterTool.fromArgs(args)

    val inputPath = params.get("input")
    val outputPath = params.get("output", "")
      val windowType = params.get("window", "tumbling")
      val parserParallelism = params.getInt("parser-parallelism", 2)
      val rankParallelism = params.getInt("rank-parallelism",2)

      val writeResultsToFile = false

      if(windowType == "tumbling"){
        executeTumbling(inputPath, outputPath,parserParallelism,rankParallelism, writeResultsToFile)
      } else if(windowType == "sliding"){
        executeSliding(inputPath, outputPath,parserParallelism,rankParallelism,writeResultsToFile)
      }




    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }

  def toLatencyComment(comment: Comment, latency: Long): LatencyComment = {
    new LatencyComment(comment.id, comment.user, comment.content,comment.timestamp, comment.postComment, comment.parentID, latency)
  }
}



class IncrementalRankMergerMetric(label: String) extends ProcessAllWindowFunction[GenericRankingResult[Long],GenericRankingResult[Long],TimeWindow] {

  val metricLabel = label
  /* keeps the starting timestamp of last window */
  var lastWindowStart = 0L

  /* keeps the ranking associated with a given timestamp */
  var hashMap : mutable.HashMap[String, GenericRankingResult[Long]] = mutable.HashMap()

  var latencyMeter = new LatencyMeter

  override def process(context: Context, elements: Iterable[GenericRankingResult[Long]], out: Collector[GenericRankingResult[Long]]): Unit = {
    val currWindow = context.window

    updateState(currWindow)

    elements.foreach {
      case metricResults: RankingResultMetric[Long] =>
        latencyMeter.addLatencyFromTimestamp(metricResults.latencyTimeStart)
        if(latencyMeter.shouldOutputLatency()){
          latencyMeter.printLatency(metricLabel)
          val rankingMetric = new RankingResultMetric[Long]("",List(),0, 0)
          rankingMetric.mean = latencyMeter.averageLatency()
          out.collect(rankingMetric)
        }
      case el =>
        if (hashMap.contains(el.timestamp)) {
          val got = hashMap(el.timestamp)
          hashMap += (el.timestamp -> got.mergeRank(el))
        } else {
          hashMap += (el.timestamp -> el)
        }
        out.collect(computeFinalRank())
    }





  }

  /**
    * updates new window timestamp
    * and clears hashmap if we are in a new
    * time window
    * @param currWindow
    */
  def updateState(currWindow: TimeWindow) = {
    if(currWindow.getStart > lastWindowStart){
      lastWindowStart = currWindow.getStart
      hashMap.clear()
      /*out.collect(new GenericRankingResult[Long]("============= NEW WINDOW with start %s and duration for %d ============= "
        .format(Parser.convertToDateString(currWindow.getStart), (currWindow.getEnd - currWindow.getStart) / 1000 / 60), List(), 0))*/
    }
  }



  /**
    * Incrementally merges ranks to have a final one
    * for that particular time window as the union
    * of ranks in smaller time windows within the
    * current larger window
    * @return
    */
  def computeFinalRank(): GenericRankingResult[Long] = {
    var finalIncrementalRank : GenericRankingResult[Long] = null
    for( (k,v) <- hashMap){
      if(finalIncrementalRank == null){
        finalIncrementalRank = v
      } else {
        finalIncrementalRank = finalIncrementalRank.incrementalMerge(v)
      }
    }
    finalIncrementalRank
  }


}

class MetricFlatMap extends FlatMapFunction[String, LatencyComment]{
  override def flatMap(value: String, out: Collector[LatencyComment]): Unit = {
    val comm = toLatencyComment(Parser.parseComment(value).get, System.currentTimeMillis())
    if(comm.isPostComment()) { out.collect(comm) }
  }
}

class MetricOutputSelector extends OutputSelector[GenericRankingResult[Long]] {
  override def select(value: GenericRankingResult[Long]): lang.Iterable[String] = {
    val array = new util.ArrayList[String]()
    if(value.isInstanceOf[RankingResultMetric[Long]]){
      array.add("metric")
    }else {
      array.add("rank")
    }
    array
  }
}




class GlobalRankerMetric(label: String) extends ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]] {

  private val metricLabel = label

  /* for how much millis to keep partial rankings */
  private val delta = 1000 * 60 * 10 /* 10 minutes */

  /* keeps the partial rankings and computes global one */
  private var globalRankingHolder: GlobalRankingHolder[Long] = new GlobalRankingHolder[Long](delta)

  /* keeps last seen timestamp as to filter
   * incoming late partial rankings */
  private var lastTimestampLong = 0L

  /* keeps last sent ranking to avoid outputting duplicates */
  private var lastSentRanking : GenericRankingResult[Long] = new GenericRankingResult[Long]("", List(),10)

  /* measures latency */
  val latencyMeter = new LatencyMeter

  override def processElement(value: GenericRankingResult[Long],
                              ctx: ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]]#Context,
                              out: Collector[GenericRankingResult[Long]]): Unit = {


    /* current partial ranking timestamp */
    val timestamp = value.timestamp
    val timestampMillis = Parser.millisFromStringDate(timestamp)



    /*value match {
      case latency: RankingResultMetric[Long] =>
        latencyMeter.addLatencyFromTimestamp(latency.latencyTimeStart)
        if(latencyMeter.shouldOutputLatency()){
          latencyMeter.printLatency(metricLabel)
          val rankingMetric = new RankingResultMetric[Long](lastSentRanking.timestamp,List(),0, 0)
          rankingMetric.mean = latencyMeter.averageLatency()
          out.collect(rankingMetric)
        }

      case _ =>
        if(timestampMillis < lastTimestampLong){
          /* clear old partial rankings */
          globalRankingHolder.clearOldRankings(currentTimestamp = timestampMillis)
        } else {
          val globalRanking = globalRankingHolder.globalRanking(partialRanking = value)
          if(notPreviouslySent(globalRanking)){
            /* output global rank computes using current partial ranking */
            out.collect(globalRanking)
            lastSentRanking = globalRanking
          }

          /* update last seen timestamp with current */
          lastTimestampLong = timestampMillis
        }
    } */

    if(timestampMillis < lastTimestampLong){
      /* clear old partial rankings */
      globalRankingHolder.clearOldRankings(currentTimestamp = timestampMillis)
    } else {
      val globalRanking = globalRankingHolder.globalRanking(partialRanking = value)
      if(notPreviouslySent(globalRanking)){
        /* output global rank computes using current partial ranking */
        out.collect(globalRanking)
        lastSentRanking = globalRanking
      }

      /* update last seen timestamp with current */
      lastTimestampLong = timestampMillis
    }

    latencyMeter.addLatencyFromTimestamp(value.asInstanceOf[RankingResultMetric[Long]].latencyTimeStart)
    if(latencyMeter.shouldOutputLatency()){
      latencyMeter.printLatency(metricLabel)
    }

  }

  /* checks if ranking has already been sent */
  def notPreviouslySent(globalRanking: GenericRankingResult[Long]): Boolean = globalRanking != lastSentRanking
}



/**
  * A partial ranking operator that is combined with an
  * AggregatedFunction to incrementally aggregate elements
  * as they arrive in the current time window and update their ranking.
  * Outputs ranking only if it has changed from the previously sent one.
  * It also clear the ranking if the sliding time window advances
  */
class PartialRankerMetric
  extends ProcessWindowFunction[(Long,Score), GenericRankingResult[Long], Long, TimeWindow]{

  /* keeps the partial ranking of the current operator instance */
  var partialRankBoard : GenericRankingBoard[Long] = new GenericRankingBoard[Long]()

  /* keeps the last time window start to compare it with new ones
   * and know when to clear ranking for new window */
  var lastTimeWindowStart : Long = 0


  /**
    * called when new value elements are ready
    * to be processed in the current time window
    * for the specific key
    * @param key  postID or userID
    * @param context
    * @param elements scores
    * @param out
    */
  override def process(key: Long, context: Context, elements: Iterable[(Long, Score)], out: Collector[GenericRankingResult[Long]]): Unit = {
    /* get current time window */
    val currentTimeWindow = context.window
    updateState(currentTimeWindow)

    val currTuple = elements.iterator.next()

    incrementScore(key,currTuple._2)

    /*if(currTuple._1 != 0L){
      out.collect(new RankingResultMetric[Long](Parser.convertToDateString(currentTimeWindow.getStart), List(), 0, currTuple._1))
    } */

    outputPartialRankingIfChanged(out, currentTimeWindow, currTuple._1)
  }

  /**
    * outputs the current partial ranking to the next
    * operator only if the ranking has changed and it's
    * not empty
    * @param out collector that forwards data to the next operator
    * @param currentWindow time window
    */
  def outputPartialRankingIfChanged(out: Collector[GenericRankingResult[Long]], currentWindow: TimeWindow, latencyStart: Long) = {
    if(partialRankBoard.rankHasChanged()) {

      val ranking = partialRankBoard.topK() /* Top-K in current window */
      if(ranking.nonEmpty) {
        /* sets the statistic's start timestamp as the current window's start time */
        val windowStartTimeStamp = Parser.convertToDateString(currentWindow.getStart)
        /* outputs the current partial Top-K ranking with its relative start timestamp */
        val output = new RankingResultMetric[Long](windowStartTimeStamp, ranking, partialRankBoard.K, latencyStart)
        out.collect(output)
      }
    }
  }

  /**
    * updates last time window start time if the current one
    * is starting after the last one and clears the
    * ranking board so to insert only scores within
    * the current window
    * @param currentTimeWindow TimeWindow
    */
  def updateState(currentTimeWindow: TimeWindow) = {

    if(currentTimeWindow.getStart > lastTimeWindowStart){
      lastTimeWindowStart = currentTimeWindow.getStart
      partialRankBoard.clear()
    }
  }

  /**
    * increments the ranking score associated
    * with the key
    * @param key
    * @param score
    */
  def incrementScore(key: Long, score: Score) = {
    partialRankBoard.incrementScoreBy(key, score)
  }


}


class RankingResultMetric[A](startTime: String, elements : List[GenericRankElement[A]], k: Int, latency: Long)
  extends GenericRankingResult[A](startTime, elements, k) with LatencyObject {
  this.latencyTimeStart = latency
  var mean : Double =  0d

  override def toString: String = s"$mean"
}


trait LatencyObject {
  var latencyTimeStart = 0L
}

class LatencyComment(commId: Long, commenter: User, body: String, time: DateTime, commentToPost: Boolean, parent: Long, latency: Long)
  extends Comment(commId,commenter,body,time,commentToPost, parent) with LatencyObject {

  this.latencyTimeStart = latency

}


class SimpleScoreAggregatorMetric extends AggregateFunction[(Long, SimpleScore, Long), (Long,Score), (Long,Score)]{
  override def createAccumulator(): (Long, Score) = (0L, SimpleScore(0))

  override def add(value: (Long, SimpleScore, Long), accumulator: (Long, Score)): (Long, Score) = {
    (if(value._3 > accumulator._1) value._3 else accumulator._1, value._2.add(accumulator._2))
  }

  override def getResult(accumulator: (Long, Score)): (Long, Score) = accumulator

  override def merge(a: (Long, Score), b: (Long, Score)): (Long, Score) = {
    (if(a._1 > b._1) a._1 else b._1, a._2.add(b._2))
  }
}
