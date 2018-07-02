package utils.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._


object StreamFilter {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  //  val data: DataStream[String] = env.fromElements("2010-02-03T16:35:50.015+0000|1564|3825",
  //    "2010-02-05T18:37:22.634+0000|2608|3633",
  //    "2010-02-08T17:34:27.742+0000|2608|2886",
  //    "2010-02-03T16:35:50.015+0000|3825|1564"
  //  )

  val data: DataStream[String] = env.readTextFile("dataset/friendships.dat")


  def filter(ds: DataStream[String]): DataStream[(String, String, String)] = {

    val res = data.
      mapWith(line => {
        val str = line.split("\\|")
        if (str(1).toLong > str(2).toLong)
          (str(0), str(1), str(2))
        else
          (str(0), str(2), str(1))
      })
      .keyBy(tuple => (tuple._2, tuple._3))
      .flatMap(new FilterFunction())

    res
  }

  def main(args: Array[String]): Unit = {
    filter(data)
      .map(connection => println(connection._2, connection._3))

    env.execute("filtering")
  }
}
