import java.lang
import java.util.concurrent.TimeUnit

import QueryTwo.env
import model.UserConnection
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.joda.time.{DateTime, DateTimeZone}
import utils.Parser

object QueryThree {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val friendshipData = env.readTextFile("dataset/friendships.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromFriendship(line), 1))

  val commentsData = env.readTextFile("dataset/comments.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromComment(line), 1) )  //.flatMap{ Parser.parseComment(_)}

  val postsData = env.readTextFile("dataset/posts.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromPost(line), 1) )    //.flatMap{ Parser.parsePost(_)}

  def main(args: Array[String]) : Unit = {

    val union = postsData.union(commentsData, friendshipData)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .aggregate(new MyAggregate, new MyProcess)
      .process(new RankProcessFunction)
      .setParallelism(2)
      .process(new MergeRankProcessFunction)
      .writeAsText("results/q3")


    /*val union = postsData.union(commentsData, friendshipData)
      .assignAscendingTimestamps(tuple => Parser.extractTimeStamp(tuple._2))
      .map{ tuple =>
        var outTuple = (1L, 1)
        tuple._1 match {
          case 1 => outTuple = (Parser.userIDFromFriendship(tuple._2), 1)
          case 2 => outTuple = (Parser.userIDFromComment(tuple._2), 1)
          case 3 => outTuple = (Parser.userIDFromPost(tuple._2), 1)
        }
        outTuple
      }
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .aggregate(new MyAggregate, new MyProcess)
      .process(new RankProcessFunction)
      .setParallelism(2)
      .process(new MergeRankProcessFunction)
      .writeAsText("results/q3") */

    /*friendshipData.connect(union)
      .map(new CoMapFunction[UserConnection, String, String] {
        override def map1(value: UserConnection): String = ???

        override def map2(value: String): String = ???
      }) */

    /*friendshipData.union(commentsData,postsData)
      .map(el => if(el == "2010-02-03T16:35:50.015+0000|1564|3825"){println(el)})

      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new ProcessAllWindowFunction[String,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          elements.foreach(println(_))
        }
      }) */
    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }

  class MergeRankProcessFunction extends ProcessFunction[RankingResult[Long], RankingResult[Long]]{

    println("Merge Rank Operator in Thread " + Thread.currentThread().getId)
    override def processElement(value: RankingResult[Long],
                                ctx: ProcessFunction[RankingResult[Long], RankingResult[Long]]#Context,
                                out: Collector[RankingResult[Long]]): Unit = {
      out.collect(value)
    }
  }

  class RankProcessFunction extends ProcessFunction[(Long, Int), RankingResult[Long]]{

    var lastWatermark : Long = 0
    var rankingBoard : RankingBoard[Long] = _

    override def processElement(value: (Long, Int), ctx: ProcessFunction[(Long, Int), RankingResult[Long]]#Context, out: Collector[RankingResult[Long]]): Unit = {


      if(rankingBoard == null){
        rankingBoard = new RankingBoard[Long]()
        println("Ranking board init for thread " + Thread.currentThread().getId)

      }
      val currentWatermark = ctx.timerService().currentWatermark()
      if(currentWatermark < 0) {
        println("Watermark is negative")
      }

      rankingBoard.incrementScoreBy(value._1, value._2)

      if(rankingBoard.rankHasChanged()) {
        val ranking = rankingBoard.topK()
        if(ranking.size == rankingBoard.K) {
          val output = new RankingResult[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), ranking, rankingBoard.K)
          println("Operator " + Thread.currentThread().getId + " output " +output)
          out.collect(output)
        }
      }
      //println("Score of " + rankingBoard.scoreOf(120260221010L))


      if(currentWatermark > lastWatermark){
        if(lastWatermark != 0){
          // out.collect(new Result[Long](new DateTime(currentWatermark).toDateTime(DateTimeZone.UTC).toString(), rankingBoard.topK()))
        }
        lastWatermark = currentWatermark
        rankingBoard.clear()
      }
    }
  }

  class MyProcess extends ProcessWindowFunction[Int, (Long, Int), Long, TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Int], out: Collector[(Long, Int)]): Unit = {
      val value = elements.iterator.next()
      out.collect( (key, value) )
    }
  }

  class MyAggregate extends AggregateFunction[(Long, Int), Int, Int]{
    override def createAccumulator(): Int = {
      0
    }

    override def add(value: (Long, Int), accumulator: Int): Int = {
      value._2 + accumulator
    }

    override def getResult(accumulator: Int): Int = {
      accumulator
    }

    override def merge(a: Int, b: Int): Int = {
      a + b
    }
  }


}
