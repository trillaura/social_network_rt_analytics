import java.lang
import java.util.Properties
import java.util.concurrent.TimeUnit

import QueryOne.env
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

  def executeWindowAll() : Unit = {
    val results = data
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps(_.timestamp.getMillis )
      .map(postComment => (postComment.parentID, 1))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
      .process(new WindAFunction)
      .setParallelism(2)
      .process(new MergeRank)

  }



  def executeParallel() : Unit = {
    val hourlyResults = data
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps( _.timestamp.getMillis )
      .map(postComment => (postComment.parentID, 1))
      //.map(postComment => GenericRankElement[Long](id = postComment.parentID, score = SimpleScore(1)))
      //.keyBy(_.id)
      .keyBy(_._1)
      //.timeWindow(Time.hours(1))
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .reduce( new SumReduceFunction , new PartialRankProcessWindowFunction)
      .setParallelism(2)
      //.setParallelism(4)
      //.process(new TopKProcessFunction(10))
      //.setParallelism(2)
      .process(new MergeRank)
      .setParallelism(1)



    hourlyResults.writeAsText("results/q2-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
      .reduce(_ mergeRank _)

    dailyResults.writeAsText("results/q2-daily")


    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce(_ mergeRank _)

    weeklyResults.writeAsText("results/q2-weekly")

  }

  def main(args: Array[String]) : Unit = {

    executeParallel()

    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }
}



class MergeRank extends ProcessFunction[RankingResult[Long], RankingResult[Long]] {

  var hashMap : mutable.HashMap[String, RankingResult[Long]] = _
  var lastTimestampLong = 0L
  var lastTimestampString = ""

  override def processElement(value: RankingResult[Long],
                              ctx: ProcessFunction[RankingResult[Long], RankingResult[Long]]#Context,
                              out: Collector[RankingResult[Long]]): Unit = {

    if(hashMap == null){
      hashMap = mutable.HashMap[String,  RankingResult[Long]]()
      println("Merge Rank started in Thread " + Thread.currentThread().getId)
    }

    val timestamp = value.timestamp
    if(Parser.millisFromStringDate(timestamp) < lastTimestampLong){
      println("Timestamp already passed")
      hashMap.remove(timestamp) /* remove old ranking */
    } else {

      if (!hashMap.contains(timestamp)) {
        /* new ranking, no previous one present */
        hashMap += (timestamp -> value)
        out.collect(value)
      } else {
        val previousRanking = hashMap(timestamp)
        val mergedRank = previousRanking.mergeRank(value)
        hashMap += (timestamp -> mergedRank)
        out.collect(mergedRank)
      }

      lastTimestampString = timestamp
      lastTimestampLong = Parser.millisFromStringDate(lastTimestampString)

    }
  }
}


class PartialRankProcessWindowFunction extends ProcessWindowFunction[(Long, Int), RankingResult[Long], Long, TimeWindow]{

  var partialRankBoard : RankingBoard[Long] = _
  var lastWatermark : Long = 0

  override def process(key: Long,
                       context: Context,
                       elements: Iterable[(Long, Int)],
                       out: Collector[RankingResult[Long]]): Unit = {
    if(partialRankBoard == null){
      partialRankBoard = new RankingBoard[Long]()
      println("Partial ranking board init for Operator in Thread " + Thread.currentThread().getId)
    }

    val currentWatermark = context.currentWatermark
    val date = new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString()

    val value = elements.iterator.next()
    partialRankBoard.incrementScoreBy(value._1, value._2)

    if(date.contains("2012")){
      println("2012")
    }

    if(partialRankBoard.rankHasChanged()) {
      val ranking = partialRankBoard.topK()
      //if(ranking.size == partialRankBoard.K) {



        val timestamp = new DateTime(currentWatermark).toString() //.toDateTime(DateTimeZone.UTC).toString()
        val output = new RankingResult[Long](timestamp, ranking, partialRankBoard.K)
        //println("Operator " + Thread.currentThread().getId + " output " +output)
        out.collect(output)
      //}
    }

    if(currentWatermark > lastWatermark){
      if(lastWatermark != 0){
        // out.collect(new Result[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), rankingBoard.topK()))
        val ranking = partialRankBoard.topK()
        val timestamp = new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString()
        val output = new RankingResult[Long](timestamp, ranking, partialRankBoard.K)
      }
      lastWatermark = currentWatermark
      partialRankBoard.clear()
    }
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
    //println("Score of " + rankingBoard.scoreOf(120260221010L))


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

class SumReduceFunction extends ReduceFunction[(Long, Int)]{
  override def reduce(value1: (Long, Int), value2: (Long, Int)): (Long, Int) = {
    if(value1._1 != value2._1){println("Different")}
    (value1._1, value1._2 + value2._2)
  }
}

class WindAFunction extends ProcessAllWindowFunction[(Long, Int), RankingResult[Long], TimeWindow] {

  var board : RankingBoard[Long] = _


  override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[RankingResult[Long]]): Unit = {

    val windowStart = new DateTime(context.window.getStart).toDateTime(DateTimeZone.UTC)
    val windowEnd = new DateTime(context.window.getEnd).toDateTime(DateTimeZone.UTC)
    //println("Window Start " + windowStart)
    //println("Window End " + windowEnd)
    if(board == null){
      println("Initializing Ranking Board")
      board = new RankingBoard[Long]()
    }
    board.clear()
    elements.foreach( el => {
      board.incrementScoreBy(el._1, el._2)
      if(board.rankHasChanged()){
        out.collect(new RankingResult[Long](windowStart.toString(), board.topK(), board.K))
      }
    })
  }
}




