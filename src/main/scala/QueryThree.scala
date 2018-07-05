import java.util.concurrent.TimeUnit
import flink_operators.{GlobalRanker, PartialRanker, UserScoreAggregator}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import utils.ranking.UserScore
import utils.Parser


object QueryThree {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val friendshipData = env.readTextFile("dataset/friendships.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromFriendship(line), UserScore(1,0,0) ))

  val commentsData = env.readTextFile("dataset/comments.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromComment(line), UserScore(0,0,1) ))

  val postsData = env.readTextFile("dataset/posts.dat")
    .assignAscendingTimestamps(t => Parser.extractTimeStamp(t))
    .map( line => (Parser.userIDFromPost(line), UserScore(0,1,0) ))

  def main(args: Array[String]) : Unit = {

    val hourlyResults = postsData.union(commentsData, friendshipData)
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
      .aggregate(new UserScoreAggregator, new PartialRanker)
      .process(new GlobalRanker)

    hourlyResults.writeAsText("results/q3-hourly")

    val dailyResults = hourlyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1) ))
      .reduce(_ mergeRank _)

    dailyResults.writeAsText("results/q3-daily")

    val weeklyResults = dailyResults
      .assignAscendingTimestamps(res => Parser.millisFromStringDate(res.timestamp))
      .windowAll(SlidingEventTimeWindows.of(Time.days(7),Time.days(1)))
      .reduce(_ mergeRank _)

    weeklyResults.writeAsText("results/q3-weekly")

    val executingResults = env.execute()
    println("Query 3 Execution took " + executingResults.getNetRuntime(TimeUnit.SECONDS) + " seconds")
  }




}
