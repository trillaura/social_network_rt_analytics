import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import utils.ranking.{Score, UserScore}
import utils.Parser


object QueryThreeMetric {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)
  env.getConfig.setLatencyTrackingInterval(5L)


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
  def executeTumbling(friendshipStream : DataStream[(Long, UserScore,Long)],
                       commentStream: DataStream[(Long, UserScore,Long)], postStream: DataStream[(Long, UserScore,Long)]
                       ,parserParallelism: Int, rankParallelism: Int) : Unit = {



    val hourlyResults = postStream.union(commentStream, friendshipStream)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1) ))
      .aggregate(new UserScoreAggregatorMetric, new PartialRankerMetric)
      .setParallelism(rankParallelism)
      .process(new GlobalRankerMetric("Hourly"))
      .setParallelism(1)

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24) ))
      .process(new IncrementalRankMergerMetric("Daily"))


    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(TumblingEventTimeWindows.of(Time.days(7) ))
      .process(new IncrementalRankMergerMetric("Weekly"))



  }

  /**
    * Executes the query using sliding windows
    */
  def executeSliding( friendshipStream : DataStream[(Long, UserScore,Long)],
  commentStream: DataStream[(Long, UserScore,Long)], postStream: DataStream[(Long, UserScore,Long)]
                      , parserParallelism: Int, rankParallelism: Int) : Unit = {


    val keyedUnitedStream =
      postStream.union(commentStream, friendshipStream)
      .setParallelism(parserParallelism)
        .keyBy(_._1)

    val hourlyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new UserScoreAggregatorMetric,new PartialRankerMetric)
      .setParallelism(rankParallelism * 2)
      .process(new GlobalRankerMetric("Hourly"))
      .setParallelism(1)


    val dailyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
      .aggregate(new UserScoreAggregatorMetric,new PartialRankerMetric)
      .setParallelism(rankParallelism * 2 )
      .process(new GlobalRankerMetric("Daily"))
      .setParallelism(1)


    val weeklyResults = keyedUnitedStream
      .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
      .aggregate(new UserScoreAggregatorMetric,new PartialRankerMetric)
      .setParallelism(rankParallelism)
      .process(new GlobalRankerMetric("Weekly"))
      .setParallelism(1)

  }

  /**
    * --posts /posts.dat --comments /comments.dat --friendships /friendships.dat
    * @param args
    */
  def main(args: Array[String]) : Unit = {

    val params : ParameterTool = ParameterTool.fromArgs(args)

    val comments = params.get("comments",utils.Configuration.DATASET_COMMENTS)
    val posts = params.get("posts", utils.Configuration.DATASET_POSTS)
    val friendships = params.get("friendships", utils.Configuration.DATASET_FRIENDSHIPS)

    val parserParallelism = params.getInt("parser-parallelism", 2)
    val rankParallelism = params.getInt("rank-parallelism",2)

    val windowType = params.get("window", "tumbling")

    val friendshipData: DataStream[(Long, UserScore,Long)] =
      env.readTextFile(friendships)
      .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
      .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0),System.nanoTime() ))
      .setParallelism(parserParallelism)

    val commentsData : DataStream[(Long, UserScore,Long)] =
      env.readTextFile(comments)
      .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
      .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ,System.nanoTime()))
        .setParallelism(parserParallelism)

    val postsData: DataStream[(Long, UserScore,Long )] =
      env.readTextFile(posts)
      .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
        .setParallelism(parserParallelism)
      .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) , System.nanoTime()))
        .setParallelism(parserParallelism)

    if(windowType == "tumbling"){
      executeTumbling(friendshipData, commentsData, postsData,parserParallelism, rankParallelism)
    } else if(windowType == "sliding"){
      executeSliding(friendshipData, commentsData, postsData,parserParallelism, rankParallelism)
    }



    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
    println(executingResults.getAllAccumulatorResults)



  }

}


class UserScoreAggregatorMetric extends AggregateFunction[(Long, UserScore, Long), (Long,Score), (Long,Score)]{
  override def createAccumulator(): (Long, Score) = (0L, UserScore(0,0,0))

  override def add(value: (Long, UserScore, Long), accumulator: (Long, Score)): (Long, Score) = {
    (if(value._3 > accumulator._1) value._3 else accumulator._1, value._2.add(accumulator._2))
  }

  override def getResult(accumulator: (Long, Score)): (Long, Score) = accumulator

  override def merge(a: (Long, Score), b: (Long, Score)): (Long, Score) = {
    (if(a._1 > b._1) a._1 else b._1, a._2.add(b._2))
  }
}

