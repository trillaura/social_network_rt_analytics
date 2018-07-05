import java.lang
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.joda.time.{DateTime, DateTimeZone}
import utils.{Configuration, Parser}
import utils.flink.{CommentsAvroDeserializationSchema, FriedshipAvroDeserializationSchema}

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable
import java.util.Properties

import utils.ranking.{RankingBoard, RankingResult, Score, SimpleScore}


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
      .aggregate(new SimpleScoreAggregator,new PartialRankProcessWindowFunction)
      .setParallelism(2)
      .process(new MergeRank)
      .setParallelism(1)



    hourlyResults.writeAsText("results/q2-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24) ))//, Time.hours(1)))
      .reduce(_ mergeRank _)
      /*.keyBy(_.timestamp)
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .reduce(new RankingResultsReducer, new PartialRankingMerger)
      .setParallelism(2)
      .process(new MergeRank) */



    dailyResults.writeAsText("results/q2-daily")


    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)) )//, Time.days(1)))
      .reduce(_ mergeRank _)

    weeklyResults.writeAsText("results/q2-weekly")

  }

  def main(args: Array[String]) : Unit = {

    executeParallel()

    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }
}

class PartialRankingMerger extends ProcessWindowFunction[GenericRankingResult[Long], GenericRankingResult[Long], String, TimeWindow]{

  /* keeps the last time window start to compare it with new ones
   * and know when to clear ranking for new window */
  var lastTimeWindowStart : Long = 0

  var currentRanking : GenericRankingResult[Long] = _
  var lastSentRanking : GenericRankingResult[Long] = _


  override def process(key: String,
                       context: Context,
                       elements: Iterable[GenericRankingResult[Long]],
                       out: Collector[GenericRankingResult[Long]]): Unit = {
    /* get current time window */
    val currentTimeWindow = context.window

    updateCurrentRanking(elements.iterator.next())

    if(differentWindow(currentTimeWindow)){
      if(lastSentRanking == null){
        lastSentRanking = currentRanking
      }
      if(lastSentRanking != currentRanking){
        out.collect(currentRanking)
        lastSentRanking = currentRanking
      }

      currentRanking = null
      lastTimeWindowStart = currentTimeWindow.getStart
    }

  }

  /**
    * checks whether we are in a new window
    * @param currentTimeWindow
    * @return
    */
  def differentWindow(currentTimeWindow: TimeWindow): Boolean = {
    currentTimeWindow.getStart > lastTimeWindowStart
  }

  /**
    * updates current window ranking by merging it
    * with new one
    * @param ranking newly arrived ranking
    */
  def updateCurrentRanking(ranking: GenericRankingResult[Long]) = {
    if(currentRanking == null){
      currentRanking = ranking
    } else {
      currentRanking = currentRanking mergeRank ranking
    }
  }

}

/**
  * Reduces Rankings by merging them
  */
class RankingResultsReducer extends ReduceFunction[GenericRankingResult[Long]]{
  override def reduce(value1: GenericRankingResult[Long], value2: GenericRankingResult[Long]): GenericRankingResult[Long] = {
    value1 mergeRank value2
  }
}

/**
  * Aggregates incoming scores for given key to get
  * total score
  */
class SimpleScoreAggregator extends AggregateFunction[(Long, SimpleScore), Score, Score]{
  override def createAccumulator(): Score = SimpleScore(0)
  override def add(value: (Long, SimpleScore), accumulator: Score): Score = accumulator.add(value._2)
  override def getResult(accumulator: Score): Score = accumulator
  override def merge(a: Score, b: Score): Score = a.add(b)
}


/**
  * Merges partial ranks into global one
  * coming from different operators.
  * It also keeps only recent partial ranks,
  * periodically discarding old ones
  */
class MergeRank extends ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]] {


  //private var hashMap =  mutable.HashMap[String,  GenericRankingResult[Long]]()

  /* for how much millis to keep partial rankings */
  private val delta = 1000 * 60 * 10 /* 10 minutes */

  /* keeps the partial rankings and computes global one */
  private var globalRankingHolder: GlobalRankingHolder[Long] = new GlobalRankingHolder[Long](delta)

  /* keeps last seen timestamp as to filter
   * incoming late partial rankings */
  private var lastTimestampLong = 0L

  /* keeps last sent ranking to avoid outputting duplicates */
  private var lastSentRanking : GenericRankingResult[Long] = new GenericRankingResult[Long]("", List(),10)


  override def processElement(value: GenericRankingResult[Long],
                              ctx: ProcessFunction[GenericRankingResult[Long], GenericRankingResult[Long]]#Context,
                              out: Collector[GenericRankingResult[Long]]): Unit = {


    /* current partial ranking timestamp */
    val timestamp = value.timestamp
    val timestampMillis = Parser.millisFromStringDate(timestamp)


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
class PartialRankProcessWindowFunction
  extends ProcessWindowFunction[Score, GenericRankingResult[Long], Long, TimeWindow]{

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
  override def process(key: Long,
                       context: Context,
                       elements: Iterable[Score],
                       out: Collector[GenericRankingResult[Long]]): Unit = {


    /* get current time window */
    val currentTimeWindow = context.window
    updateState(currentTimeWindow)

    incrementScore(key,elements.iterator.next())

    outputPartialRankingIfChanged(out, currentTimeWindow)

  }

  /**
    * outputs the current partial ranking to the next
    * operator only if the ranking has changed and it's
    * not empty
    * @param out collector that forwards data to the next operator
    * @param currentWindow time window
    */
  def outputPartialRankingIfChanged(out: Collector[GenericRankingResult[Long]], currentWindow: TimeWindow) = {
    if(partialRankBoard.rankHasChanged()) {

      val ranking = partialRankBoard.topK() /* Top-K in current window */
      if(ranking.nonEmpty) {
        /* sets the statistic's start timestamp as the current window's start time */
        val windowStartTimeStamp = Parser.convertToDateString(currentWindow.getStart)
        /* outputs the current partial Top-K ranking with its relative start timestamp */
        val output = new GenericRankingResult[Long](windowStartTimeStamp, ranking, partialRankBoard.K)
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



class TopKProcessFunction(numK : Int) extends ProcessFunction[(Long, Int), RankingResult[Long]] with CheckpointedFunction {

  var k = numK
  var state : ListState[(Long, Int)] = _
  var stateLastWatermark : ListState[Long] = _
  var lastWatermark : Long = 0

  //var pq : mutable.PriorityQueue[(Long,Int)] = mutable.PriorityQueue()(ordering)

  var rankingBoard : RankingBoard[Long] = _

  override def processElement(value: (Long, Int),
                              ctx: ProcessFunction[(Long, Int), RankingResult[Long]]#Context,
                              out: Collector[RankingResult[Long]]): Unit = {

    if(rankingBoard == null){
      rankingBoard = new RankingBoard[Long]()
      println("Ranking board init for Operator in Thread " + Thread.currentThread().getId)
    }

    val currentWatermark = ctx.timerService().currentWatermark()

    rankingBoard.incrementScoreBy(value._1, value._2)

    if(rankingBoard.rankHasChanged()) {
      out.collect(new RankingResult[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), rankingBoard.topK(), rankingBoard.K))
    }
    //println("utils.ranking.Score of " + rankingBoard.scoreOf(120260221010L))


    if(currentWatermark > lastWatermark){
      if(lastWatermark != 0){
       // out.collect(new Result[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), rankingBoard.topK()))
      }
      lastWatermark = currentWatermark
      rankingBoard.clear()
    }

    //out.collect(orderedSet.max)

  }


  def topKPriority(queue : mutable.PriorityQueue[(Long, Int)], k: Int ) : ListBuffer[(Long, Int)] = {
    val topList : ListBuffer[(Long, Int)] = ListBuffer()

    for(i <- 0 until k){
      topList += queue.dequeue()
    }

    topList
  }


  override def snapshotState(context: FunctionSnapshotContext): Unit = {

    println("State snapshot")
    state.clear()
    /*for(el <- orderedSet){
      state.add(el)
    } */
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    println("State init")

    val descriptor = new ListStateDescriptor[(Long, Int)](
      "ranking",
      TypeInformation.of(new TypeHint[(Long, Int)]() {})
    )

    state = context.getOperatorStateStore.getListState(descriptor)


    val lastWatermarkStateDescriptor = new ListStateDescriptor[Long](
      "lastWatermark",
      TypeInformation.of(new TypeHint[Long] {})
    )

    stateLastWatermark = context.getOperatorStateStore.getListState(lastWatermarkStateDescriptor)


    if(context.isRestored) {
      println("Context Restored")
      /*for(el <- state.get()){
        orderedSet += el
      }*/
    }


  }


}


class CountAndTopK extends ProcessWindowFunction[(Long, Int), (Long, Int), Long, TimeWindow] with CheckpointedFunction {


  var state : ValueState[mutable.TreeSet[(Long, Int)]] = _

  override def process(key: Long, context: Context, elements: Iterable[(Long, Int)], out: Collector[(Long, Int)]): Unit = {

    val tmpCurrentState = state.value

    var currentState : mutable.TreeSet[(Long, Int)] =
      if(tmpCurrentState != null){
      tmpCurrentState
    } else {
      val ordering = Ordering[Int].on[(Long, Int)](_._2)
      mutable.TreeSet()(ordering)
    }

    var count = 0
    for(el <- elements){
      count += 1
    }

    val ele = (key, count)
    currentState += ele

    state.update(currentState)

    /* ? */
    if(context.currentProcessingTime > context.currentWatermark){
      out.collect(currentState.max)
    }
  }

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(
      new ValueStateDescriptor[mutable.TreeSet[(Long, Int)]]("ranking",
        createTypeInformation[mutable.TreeSet[(Long, Int)]])
    )
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = ???

  override def initializeState(context: FunctionInitializationContext): Unit = ???
}

class SumReduceFunction extends ReduceFunction[(Long, Score)]{
  override def reduce(value1: (Long, Score), value2: (Long, Score)): (Long, Score) = {
    (value1._1, value1._2.add(value2._2))
  }
}






