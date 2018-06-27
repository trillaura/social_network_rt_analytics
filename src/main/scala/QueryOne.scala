import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import utils.{CountAggregation, Parser}

object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")

  def main(args: Array[String]): Unit = {


//        val res = data
//          .mapWith { line => Parser.parseUserConnection(line).get }
//          .mapWith(connection => (connection.timestamp.getMillis, connection.timestamp.hourOfDay().get(), 1))
//          .assignAscendingTimestamps(tuple => tuple._1)
//          .keyBy(1)
//          .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
//          .sum(2)
//          .map(line => println(line._1, line._2, line._3))


    val res = data
      .mapWith { line => Parser.parseUserConnection(line).get }
      .mapWith(connection => (connection.timestamp.getMillis, connection.timestamp.hourOfDay().get(), 1))
      .assignAscendingTimestamps(tuple => tuple._1)
      .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
      .aggregate(new CountAggregation())
      .map( array => println(array.mkString(" ")))


    env.execute()
  }
}
