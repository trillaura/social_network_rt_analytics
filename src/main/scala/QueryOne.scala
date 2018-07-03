import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
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
      // Assign event time
      .assignAscendingTimestamps(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .keyBy(conn => (conn._2, conn._3))
      // Apply filtering
      .flatMap(new FilterFunction)

    /*
      HOURLY COUNT
     */
    val hourlyCnt = filtered
      .mapWith(tuple => {
        (Parser.convertToDateTime(tuple._1).getHourOfDay, 1)
      })
      .keyBy(0)
      .timeWindow(Time.minutes(60))
      .sum(1)

    /*
      Aggregate hourly count for day
      This stage receives 24 tuple for day from the previous one
    */
    val dailyCnt = hourlyCnt
      .timeWindowAll(Time.hours(24))
      .aggregate(new CountAggregation, new AddWindowStart)

    dailyCnt.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })
      .print()

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
        new AddWindowStart
      )

    /* output */
    weeklyCnts.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })
      .print()

    /*
      Perform count from the start of the social network.
      We are WORKING WITH STATE.
     */
    val totCnts = weeklyCnts
      .timeWindowAll(Time.days(30))
      .process(new CountProcessWithState())

    totCnts.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })
      .print()

  }


  def main(args: Array[String]): Unit = {
    execute(stream)
    //    stream.print()
    env.execute()
  }
}
