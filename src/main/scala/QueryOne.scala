import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import model.UserConnection
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.{DateTime, DateTimeZone}
import utils._
import utils.flink.{AddWindowStart, CountAggregation, CountProcessWithState}

object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val ds: DataStream[String] = env.fromElements("2010-02-03T16:35:50.015+0000|1564|3825",
    "2010-02-05T18:37:22.634+0000|2608|3633",
    "2010-02-08T17:34:27.742+0000|2608|2886",
    "2010-02-03T16:35:50.015+0000|3825|1564",
    "2010-02-08T17:34:27.742+0000|2886|2608"
  )

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")

  def execute(ds: DataStream[String]): Unit = {

    val filtered = data.
      mapWith(line => {
        val str = line.split("\\|")
        if (str(1).toLong > str(2).toLong)
          (str(0), str(1), str(2))
        else
          (str(0), str(2), str(1))
      })
      .assignAscendingTimestamps(tuple => Parser.convertToDateTime(tuple._1).getMillis)
//      .keyBy(conn => (conn._2, conn._3))
//      .flatMap(new FilterFunction)


    val hourlyCnt = filtered
      .mapWith(tuple => {
        (Parser.convertToDateTime(tuple._1).getHourOfDay, 1)
      })
      .keyBy(0)
      .timeWindow(Time.minutes(60))
      .sum(1)

    /* It receive 24 tuple at day and format them for output*/
    val dailyCnt = hourlyCnt
      .timeWindowAll(Time.hours(24))
      .aggregate(new CountAggregation, new AddWindowStart)

    /* output */
    //    dailyCnt.map(array => {
    //      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
    //      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    //    })
    //      .print()

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
    //    weeklyCnts.map(array => {
    //      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
    //      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    //    })
    //      .print()

    val totCnts = weeklyCnts
      .timeWindowAll(Time.days(30))
      .process(new CountProcessWithState())

    totCnts.map(array => {
      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    })
      .writeAsText("results/tot_filtered")


  }


  def main(args: Array[String]): Unit = {
    execute(data)
    env.execute()
  }
}
