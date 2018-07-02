import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.{DateTime, DateTimeZone}
import utils._
import utils.flink.{AddWindowStart, CountAggregation, CountProcessWithState, FilterFunction}

object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")

  def execute(ds: DataStream[String]): Unit = {

    /*
      Remove duplicates for bidirectional friendships
    */
    val filtered = data.
      mapWith(line => {
        // Put first the biggest user's id
        val str = line.split("\\|")
        if (str(1).toLong > str(2).toLong)
          (str(0), str(1), str(2))
        else
          (str(0), str(2), str(1))
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
      .writeAsText("results/tot_filtered")


  }


  def main(args: Array[String]): Unit = {
    execute(data)
    env.execute()
  }
}
