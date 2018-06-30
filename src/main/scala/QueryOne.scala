import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.{DateTime, DateTimeZone}
import utils._

object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")

  def execute(): Unit = {
    // Remove duplicates
    val filtered = StreamFilter.filter(data)

    /*
      Compute hourly count on 24 hours event time
     */
    val oneDayCnts = data
      .mapWith { line => Parser.parseUserConnection(line).get }
      // Assign event time
      .assignAscendingTimestamps(connection => connection.timestamp.getMillis)
      // Add field with hour index
      .map(connection => (connection, connection.timestamp.hourOfDay().get()))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
      // Count friendship in each hour and add enrich data with the window starting point
      .aggregate(new CountAggregation(), new AddWindowStart())


//    oneDayCnts
    //      .map(array => {
    //        val startWindow = new DateTime(array._1, DateTimeZone.UTC)
    //        (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    //      })
    //      .print()

    /*
      Compute hourly count on 7 days event time
     */
    val weeklyCnts = oneDayCnts
      .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
      // Sum counters index by index to get the weekly count of friendship for hour
      // Select as window starting point the maximum
      .reduce((v1, v2) => {

      var start = v1._1
      if (v2._1 < v1._1) {
        start = v2._1
      }

      val res = v1._2.zip(v2._2).map { case (x, y) => x + y }
      (start, res)
    })

    //    weeklyCnts.map(array => {
    //      val startWindow = new DateTime(array._1, DateTimeZone.UTC)
    //      (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
    //    })
    //      .print()


    /*
      Compute hourly counts from the social network start
     */
    val totCnts = weeklyCnts
      .windowAll(TumblingEventTimeWindows.of(Time.days(31)))
      // Sum counters index by index to get the weekly count of friendship for hour
      // Select as window starting point the maximum
      .process(new CountProcessWithState())

        totCnts.map(array => {
          val startWindow = new DateTime(array._1, DateTimeZone.UTC)
          (startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), array._2.mkString(" "))
        })
          .print()
  }

  def main(args: Array[String]): Unit = {
    execute()
    env.execute()
  }
}
