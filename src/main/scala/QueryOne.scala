import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.joda.time.{DateTime, DateTimeZone}
import utils._
import utils.flink._

object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", Configuration.BOOTSTRAP_SERVERS)
  properties.setProperty("zookeeper.connect", Configuration.ZOOKEEPER_SERVERS)
  properties.setProperty("group.id", Configuration.CONSUMER_GROUP_ID)

  private val stream = env
    .addSource(new FlinkKafkaConsumer011(Configuration.FRIENDS_INPUT_TOPIC, new FriedshipAvroDeserializationSchema, properties))

  /**
    * Query One implementation through flink window function.
    * The filtering step removes duplicates caused by bidirectional friendships.
    * Daily statistics are computed with tumbling window. Sliding window are involved to compute
    * weekly and global statistics
    *
    * @param ds DataStream
    */
  def executeWithSlidingWindowParallel(ds: DataStream[(String, String, String)]): Unit = {

    /*
      Remove duplicates for bidirectional friendships
    */
    val filtered = ds
      .mapWith(str => {
        // Put first the biggest user's id
        if (str._2.toLong > str._3.toLong) (str._1, str._2, str._3)
        else (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      // filtering
      .flatMap(new FilterFunction)

    val dailyCountIndividual = filtered
      // Map into (timestamp, user1_id)
      .mapWith(tuple => (new DateTime(tuple._1).getMillis, tuple._2))
      // assign event timestamp
      .assignAscendingTimestamps(tuple => tuple._1)
      // key bu user1_id
      .keyBy(tuple => tuple._2)
      .window(TumblingEventTimeWindows.of(Time.hours(24)))
      // (timestamp, user1_id) => ( user1_id, window_start, count per hourly slot)
      .aggregate(new CountDaily, new AddWindowStartDaily)
      .setParallelism(4)


    val dailyCount = dailyCountIndividual
      // Map into (start, [count00, count01 ,...])
      .mapWith(tuple => (tuple._2, tuple._3))
      .keyBy(r => r._1)
      // Aggregating the arrays of counters
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    dailyCount.mapWith(res => {
      val startWindow = new DateTime(res._1)
      println("daily - " + startWindow.toString(), res._2.mkString(" "))
    })


    // Weekly counts is performed as sum of the daily counts
    val weeklyCountIndividual =
      dailyCountIndividual
        .keyBy(userId => userId._1)
        .timeWindow(Time.days(7), Time.hours(24))
        .aggregate(new CountWeekly, new AddWindowStartWeekly)
        .setParallelism(4)

    // Aggregating weekly counters
    val weeklyCount = weeklyCountIndividual
      .keyBy(windowStart => windowStart._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    weeklyCount.mapWith(res => {
      val startWindow = new DateTime(res._1)
      println("weekly - " + startWindow.toString(), res._2.mkString(" "))
    })

    val global =
      dailyCountIndividual
        .mapWith(tuple => (tuple._2, tuple._3))
        .countWindowAll(1)
        .process(new CountWithState)
        .mapWith(res => {
          val startWindow = new DateTime(res._1)
          println("global - " + startWindow.toString(), res._2.mkString(" "))
        })


  }

  def executeWithTumblingWindowParallel(ds: DataStream[(String, String, String)]): Unit = {

    /*
      Remove duplicates for bidirectional friendships
    */
    val filtered = ds
      .mapWith(str => {
        // Put first the biggest user's id
        if (str._2.toLong > str._3.toLong)
          (str._1, str._2, str._3)
        else
          (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      .flatMap(new FilterFunction)

    val dailyCountIndividual = filtered
      .mapWith(tuple => (new DateTime(tuple._1).getMillis, tuple._2))
      .assignAscendingTimestamps(tuple => tuple._1)
      .keyBy(tuple => tuple._2)
      .window(TumblingEventTimeWindows.of(Time.hours(24)))
      // (timestamp, user1_id) => ( user1_id, window_start, count per hourly slot)
      .aggregate(new CountDaily, new AddWindowStartDaily)
      .setParallelism(4)


    println("PARALLELISM:" + dailyCountIndividual.parallelism)

    val dailyCount = dailyCountIndividual
      // Remove user'id field
      .mapWith(tuple => (tuple._2, tuple._3))
      // Compute total count per hourly slot aggregating all counts
      .keyBy(r => r._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    dailyCount.mapWith(res => {
      val startWindow = new DateTime(res._1)
      println("daily - " + startWindow.toString(), res._2.mkString(" "))
    })


    val weeklyCountIndividual =
      dailyCountIndividual
        // (user1_id, daily count start, counters)
        .keyBy(userId => userId._1)
        .timeWindow(Time.days(7))
        .aggregate(new CountWeekly, new AddWindowStartWeekly)

    println("PARALLELISM:" + weeklyCountIndividual.parallelism)

    val weeklyCount = weeklyCountIndividual
      .keyBy(windowStart => windowStart._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    weeklyCount.mapWith(res => {
      val startWindow = new DateTime(res._1)
      println("weekly - " + startWindow.toString(), res._2.mkString(" "))
    })

    val global =
      weeklyCountIndividual
        .countWindowAll(1)
        .process(new CountWithState)
        .mapWith(res => {
          val startWindow = new DateTime(res._1)
          println("global - " + startWindow.toString(), res._2.mkString(" "))
        })


  }

  def executeWithTumblingWindow(ds: DataStream[(String, String, String)]): Unit = {

    // Remove bidirectional friendships
    val filtered = ds
      .mapWith(str => {
        // Put first the biggest user's id
        if (str._2.toLong > str._3.toLong)
          (str._1, str._2, str._3)
        else
          (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      .flatMap(new FilterFunction)


    /*
      Aggregate hourly count for day
      This stage receives 24 tuple for day from the previous one
    */
    val dailyCnt = filtered
      .map(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.hours(24))
      .aggregate(new CountAggregation, new AddAllWindowStart)

    // Output
    dailyCnt.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      println("daily - " + startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })

    /*
      Aggregate hourly count for week
     */
    val weeklyCnts = dailyCnt
      .mapWith(tuple => tuple._2)
      .timeWindowAll(Time.days(7))
      .reduce(
        new ReduceFunction[Array[Int]] {
          override def reduce(value1: Array[Int], value2: Array[Int]): Array[Int] = {
            value1.zip(value2).map { case (x, y) => x + y }
          }
        },
        new AddAllWindowStart
      )

    //Output
    weeklyCnts.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      println("weekly - " + startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })

    /*
      Perform count from the start of the social network.
      We are WORKING WITH STATE.
     */
    val totCnts = weeklyCnts
      .timeWindowAll(Time.days(7))
      .process(new CountProcessWithState())

    totCnts.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      println("global - " + startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })

  }

  def main(args: Array[String]): Unit = {
    executeWithSlidingWindowParallel(stream)
    env.execute()
  }
}
