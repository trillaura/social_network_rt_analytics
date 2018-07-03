package storm.Bolt

import java.util
import java.util.Date

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseWindowedBolt.Duration
import org.apache.storm.topology.base.{BaseRichBolt, BaseWindowedBolt}
import org.apache.storm.tuple.{Fields, Tuple, Values}
import storm.utils.Window
import java.util.Calendar
import java.util.GregorianCalendar

class WindowCountBolt extends BaseRichBolt {

  @transient
  private lazy val windowConfiguration: util.HashMap[String, Duration] = new util.HashMap[String, Duration]()
  private var _collector: OutputCollector = _

  private var latestCompletedTimeframe: Long = 0
  private var windowPerPost: util.HashMap[String, Window] = new util.HashMap[String, Window]()
  private val MIN_IN_MS = 60 * 1000


  private def withWindowLength(duration: BaseWindowedBolt.Duration) = {
    if (duration.value <= 0) throw new IllegalArgumentException("Window length must be positive [" + duration + "]")
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration)
    this
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
  }

  def handleMetronomeMessage(tuple: Tuple): Unit = {
    val ts: Long = tuple.getLongByField("ts")

    val latestTimeFrame: Long = roundToCompletedMinute(ts)

    if (latestCompletedTimeframe < latestTimeFrame) {
      val elapsedMinutes: Int = Math.ceil((latestTimeFrame - latestCompletedTimeframe) / MIN_IN_MS).toInt
      val expired = new util.ArrayList[String]()

      for (postID: String <- windowPerPost.keySet()) {
        val w: Window = windowPerPost.get(postID)
        if (w != null) {


          w.moveForward(elapsedMinutes)
          val count: String = w.estimateTotal().toString
          if (w.estimateTotal() == 0) {
            expired.add(postID)
          }

          val values: Values = new Values()
          values.add(ts)
          values.add(postID)
          values.add(count)
          values.add(latestTimeFrame.toString)


          _collector.emit(values)
        }

        // Free memory
        for (elem <- expired) {
          windowPerPost.remove(elem)
        }

        latestCompletedTimeframe = latestTimeFrame
      }
    }

    _collector.ack(tuple)
  }

  def handlePostTuple(tuple: Tuple): Unit = {
    val ts: Long = tuple.getLongByField("ts")
    val id: String = tuple.getStringByField("post_commented")

    val latestTimeFrame: Long = roundToCompletedMinute(ts)

    if (latestCompletedTimeframe < latestTimeFrame) {
      val elapsedMinutes: Int = Math.ceil((latestTimeFrame - latestCompletedTimeframe) / MIN_IN_MS).toInt
      val expired = new util.ArrayList[String]()

      for (postID: String <- windowPerPost.keySet()) {
        val w: Window = windowPerPost.get(postID)
        if (w != null && postID != id) {


          w.moveForward(elapsedMinutes)
          val count: String = w.estimateTotal().toString
          if (w.estimateTotal() == 0) {
            expired.add(postID)
          }

          val values: Values = new Values()
          values.add(ts)
          values.add(postID)
          values.add(count)
          values.add(latestTimeFrame.toString)


          _collector.emit(values)
        }

        // Free memory
        for (elem <- expired) {
          windowPerPost.remove(elem)
        }

        latestCompletedTimeframe = latestTimeFrame
      }
    }

    val windowDuration: Duration = windowConfiguration.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)
    var w: Window = windowPerPost.get(id)
    if (w == null) {
      w = new Window(windowDuration.value / (1000 * 60))
      windowPerPost.put(id, w)
    }

    w.increment()

    val count: String = w.estimateTotal().toString

    val values: Values = new Values()
    values.add(ts)
    values.add(id)
    values.add(count)
    values.add(latestCompletedTimeframe.toString)

    _collector.emit(values)
    _collector.ack(tuple)

  }

  private def roundToCompletedMinute(timestamp: Long) = {
    val d = new Date(timestamp)
    val date = new GregorianCalendar
    date.setTime(d)
    date.set(Calendar.SECOND, 0)
    date.set(Calendar.MILLISECOND, 0)
    date.getTime.getTime
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("ts", "postID", "count", "start"))
  }
}
