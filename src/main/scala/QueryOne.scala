import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
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

    //    val dailyCount = filtered
    //      .mapWith(tuple => {
    //        (Parser.convertToDateTime(tuple._1).getHourOfDay, 1)
    //      })
    //      .keyBy(tuple => Tuple1(tuple._1))
    //      .timeWindow(Time.hours(24), Time.minutes(60))
    //      .reduce(new Count, new AddStart)
    //      .map(tuple => {
    //        val array = new Array[Int](24)
    //        array(tuple._2) = tuple._3
    //        (tuple._1, array)
    //      })
    //      .keyBy(tuple => tuple._1)
    //      .reduce((v1, v2) => {
    //        for (i <- v1._2.indices) {
    //          v1._2(i) += v2._2(i)
    //        }
    ////        v1._2.zip(v2._2).map { case (x, y) => x + y }
    //        (v1._1, v1._2)
    //
    //      })
    //      .setParallelism(1)
    //      .map(tuple => {
    //        val startWindow = new DateTime(tuple._1, DateTimeZone.UTC)
    //        println(startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), tuple._2.mkString(" "))
    //      })


    val dailyCount = filtered
      .mapWith(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.hours(24), Time.minutes(5))
      .aggregate(new CountAggregation, new AddAllWindowStart)
      .mapWith(res => {
        val startWindow = new DateTime(res._1, DateTimeZone.UTC)
        println(startWindow.toString("dd-MM-yyyy HH:mm:ssZ"), res._2.mkString(" "))
      })



  }


  def main(args: Array[String]): Unit = {
    execute(stream)
    //    stream.print()
    env.execute()
  }
}

