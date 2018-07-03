import java.lang
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.{DateTime, DateTimeZone}
import utils.Parser

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

  val data: DataStream[String] = env.readTextFile("dataset/comments.dat")

  def main(args: Array[String]) : Unit = {

    val results = data
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps(_.timestamp.getMillis)
      .map(postComment => (postComment.parentID, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce{ (f,s) => (f._1, f._2 + s._2) }
      .process(new TopKProcessFunction(10))
      .setParallelism(2)
      .process(new MergeRank)
      .setParallelism(1)
      .writeAsText("results/q2")

    /*val results = data
      .flatMap {  Parser.parseComment(_)  filter { _.isPostComment() } }
      .assignAscendingTimestamps(_.timestamp.getMillis )
      .map(postComment => (postComment.parentID, 1))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
      .process(new WindAFunction)
      .setParallelism(2)
      .process(new MergeRank)
      .writeAsText("results/query2-t-1") */



      //.reduce{ (f,s) => (f._1, f._2 + s._2) }
    val executingResults = env.execute()
    println("Query 2 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }
}

class MergeRank extends ProcessFunction[RankingResult[Long], RankingResult[Long]] {
  override def processElement(value: RankingResult[Long], ctx: ProcessFunction[RankingResult[Long], RankingResult[Long]]#Context, out: Collector[RankingResult[Long]]): Unit = {
    out.collect(value)
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
        out.collect(new RankingResult[Long](windowStart.toString(), board.topK()))
      }
    })
  }
}


class TopKProcessFunction(numK : Int) extends ProcessFunction[(Long, Int), RankingResult[Long]] with CheckpointedFunction {

  var k = numK
  var state : ListState[(Long, Int)] = _
  var stateLastWatermark : ListState[Long] = _
  var lastWatermark : Long = 0

  //var pq : mutable.PriorityQueue[(Long,Int)] = mutable.PriorityQueue()(ordering)

  val rankingBoard : RankingBoard[Long] = new RankingBoard[Long]()

  override def processElement(value: (Long, Int),
                              ctx: ProcessFunction[(Long, Int), RankingResult[Long]]#Context,
                              out: Collector[RankingResult[Long]]): Unit = {

    val currentWatermark = ctx.timerService().currentWatermark()

    rankingBoard.incrementScoreBy(value._1, value._2)

    if(rankingBoard.rankHasChanged()) {
      out.collect(new RankingResult[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), rankingBoard.topK()))
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

    println("Processing time " + new DateTime(context.currentProcessingTime))
    println("Current watermark " + new DateTime(context.currentWatermark))
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



class CountAggregate extends AggregateFunction[(Long, Int), (Long, Int), (Long, Int)]{
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: (Long, Int), accumulator: (Long, Int)): (Long, Int) = {
    (value._1, value._2 + accumulator._2)
  }

  override def getResult(accumulator: (Long, Int)): (Long, Int) = {
    accumulator
  }

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = {
    if(a._1 != 0){
      return (b._1, a._2 + b._2)
    }
    (a._1, a._2 + b._2)
  }
}


class TopK extends ProcessWindowFunction[(Long, Int), (Long, Int), Long, TimeWindow]{

  val ordering = Ordering[Int].on[(Long, Int)](_._2)
  val orderedSet : mutable.TreeSet[(Long, Int)] = mutable.TreeSet()(ordering)


  override def process(key: Long, context: Context, elements: Iterable[(Long, Int)], out: Collector[(Long, Int)]): Unit = {
    elements.foreach { el =>
      orderedSet += el
    }
    println("Print for key " + key.toString )
    //orderedSet.foreach( println(_))

    out.collect((1L,1))
  }
}


