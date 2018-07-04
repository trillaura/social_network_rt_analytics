import java.util.Properties

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
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


  def execute(ds: DataStream[(String, String, String)]): Unit = {

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


    val dailyCount = filtered
      .mapWith(tuple => new DateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.hours(24), Time.minutes(5))
      .aggregate(new CountAggregation, new AddAllWindowStart)
      .mapWith(res => {
        val startWindow = new DateTime(res._1)
        println("daily - " + res._1, res._2.mkString(" "))
      })


    val week = filtered
      .mapWith(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.days(7), Time.hours(1))
      .aggregate(new CountAggregation, new AddAllWindowStart)
      .mapWith(res => {
        val startWindow = new DateTime(res._1, DateTimeZone.UTC)
        println("weekly  ", startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), res._2.mkString(" "))
      })


    val global = filtered
      .mapWith(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.days(7), Time.minutes(5))
      .process(new CountProcessWithStateSliding)
      .mapWith(res => {
        val startWindow = new DateTime(res._1, DateTimeZone.UTC)
        println("global  ", startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), res._2.mkString(" "))
      })

  }

  def executeOnTumblingWindow(ds: DataStream[(String, String, String)]): Unit = {

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
      println("daily - ", startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
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
    execute(stream)
    env.execute()
  }
}
