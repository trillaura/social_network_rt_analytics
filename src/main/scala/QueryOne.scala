import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.joda.time.DateTime
import utils._
import utils.flink._

/**
  * Social network friendship analysis
  * The main function is executeWithSlidingWindowParallel.
  */
object QueryOne {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", Configuration.BOOTSTRAP_SERVERS)
  properties.setProperty("zookeeper.connect", Configuration.ZOOKEEPER_SERVERS)
  properties.setProperty("group.id", Configuration.CONSUMER_GROUP_ID)

  private val stream = env
    .addSource(new FlinkKafkaConsumer011(Configuration.FRIENDS_INPUT_TOPIC, new FriedshipAvroDeserializationSchema, properties))

  /**
    * Query One implementation through window function.
    * The filtering step removes duplicates caused by bidirectional friendships.
    * Daily statistics are computed with tumbling window. Sliding window are involved to compute
    * weekly and global statistics
    *
    * @param ds DataStream
    */
  def executeWithSlidingWindowParallel(ds: DataStream[(String, String, String)]): Unit = {

    val filtered = ds
      .mapWith(str => {
        if (str._2.toLong > str._3.toLong) (str._1, str._2, str._3) else (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      .flatMap(new FilterFunction)

    val dailyCountIndividual = filtered
      .mapWith(tuple => (new DateTime(tuple._1).getMillis, tuple._2)) // :(timestamp, user1_id)
      .assignAscendingTimestamps(tuple => tuple._1)
      .keyBy(tuple => tuple._2) // key bu user1_id
      .window(TumblingEventTimeWindows.of(Time.hours(24)))
      .aggregate(new CountDaily, new AddWindowStartDaily) // :(user1_id, window_start, count per hourly slot)

      .setParallelism(4)

    val dailyCount = dailyCountIndividual
      .mapWith(tuple => (tuple._2, tuple._3)) // :(start, [count00, count01 ,...])
      .keyBy(r => r._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    val weeklyCount =
      dailyCountIndividual
        .keyBy(userId => userId._1)
        .timeWindow(Time.days(7), Time.hours(24))
        .aggregate(new CountWeekly, new AddWindowStartWeekly)
        .setParallelism(4)
        .keyBy(windowStart => windowStart._1)
        .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    val globalCount =
      dailyCountIndividual
        .mapWith(tuple => (tuple._2, tuple._3))
        .countWindowAll(1)
        .process(new CountWithState)

    /*
     *  SING TO WRITE ON KAFKA
     */

    dailyCount.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_H24,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_H24))
    )

    weeklyCount.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_D7,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_D7))
    )

    globalCount.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME))
    )
  }

  // ============================ //
  //  ALTERNATIVE IMPLEMENTATION  //
  // ============================ //


  /**
    * Query one implementation using tumbling window.
    * Weekly statistics with parallel operator.
    *
    * @param ds : DataStream
    */
  def executeWithTumblingWindowParallel(ds: DataStream[(String, String, String)]): Unit = {

    val filtered = ds
      .mapWith(str => {
        if (str._2.toLong > str._3.toLong) (str._1, str._2, str._3)
        else (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      .flatMap(new FilterFunction)

    // statistics for daily keyed window
    val dailyCountIndividual = filtered
      .mapWith(tuple => (new DateTime(tuple._1).getMillis, tuple._2))
      .assignAscendingTimestamps(tuple => tuple._1)
      .keyBy(tuple => tuple._2)
      .window(TumblingEventTimeWindows.of(Time.hours(24))) // :( user1_id, window_start, count per hourly slot)
      .aggregate(new CountDaily, new AddWindowStartDaily)
      .setParallelism(4)

    // aggregate daily statistics
    val dailyCount = dailyCountIndividual
      .mapWith(tuple => (tuple._2, tuple._3))
      .keyBy(r => r._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    // statistics for weekly keyed window
    val weeklyCountIndividual = dailyCountIndividual
      .keyBy(userId => userId._1)
      .timeWindow(Time.days(7))
      .aggregate(new CountWeekly, new AddWindowStartWeekly)

    // aggregate weekly statistics
    val weeklyCount = weeklyCountIndividual
      .keyBy(windowStart => windowStart._1)
      .reduce((v1, v2) => (v1._1, v1._2.zip(v2._2).map { case (x, y) => x + y }))

    // statistics from the social network start
    val global = weeklyCountIndividual
      .countWindowAll(1)
      .process(new CountWithState)

    /*
     *  SING TO WRITE ON KAFKA
     */

    dailyCount.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_H24,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_H24))
    )

    weeklyCount.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_D7,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_D7))
    )

    global.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME))
    )

  }

  /**
    * Query one implementation using tumbling window and no parallelism.
    * The advantage is that it does not produce intermediate result caused by reduce operators
    *
    * @param ds : DataStream
    */
  def executeWithTumblingWindow(ds: DataStream[(String, String, String)]): Unit = {

    // Remove bidirectional friendships
    val filtered = ds
      .mapWith(str => {
        if (str._2.toLong > str._3.toLong) (str._1, str._2, str._3)
        else (str._1, str._3, str._2)
      })
      .keyBy(conn => (conn._2, conn._3))
      .flatMap(new FilterFunction)


    val dailyCnt = filtered
      .map(tuple => Parser.convertToDateTime(tuple._1).getMillis)
      .assignAscendingTimestamps(ts => ts)
      .timeWindowAll(Time.hours(24))
      .aggregate(new CountAggregation, new AddAllWindowStart)

    val weeklyCnts = dailyCnt
      .mapWith(tuple => tuple._2)
      .timeWindowAll(Time.days(7))
      .reduce(
        new ReduceFunction[Array[Int]] {
          override def reduce(value1: Array[Int], value2: Array[Int]): Array[Int] = {
            value1.zip(value2).map { case (x, y) => x + y }
          }
        },
        new AddAllWindowStart)

    val totCnts = weeklyCnts
      .timeWindowAll(Time.days(7))
      .process(new CountProcessWithState())


    /*
     *  SING TO WRITE ON KAFKA
     */

    dailyCnt.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_H24,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_H24))
    )

    weeklyCnts.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_D7,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_D7))
    )

    totCnts.addSink(
      new FlinkKafkaProducer011(
        Configuration.BOOTSTRAP_SERVERS,
        Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME,
        new ResultAvroSerializationSchemaFriendships(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME))
    )

  }

  def main(args: Array[String]): Unit = {
    executeWithSlidingWindowParallel(stream)
    env.execute()
  }
}
