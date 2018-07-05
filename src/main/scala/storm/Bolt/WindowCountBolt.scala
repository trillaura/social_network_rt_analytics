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

import scala.collection.JavaConverters._
import scala.collection.mutable

class WindowCountBolt extends BaseRichBolt {

  @transient
  private lazy val windowConfiguration: mutable.Map[String, Duration] = new mutable.HashMap[String, Duration]()

  private var _collector: OutputCollector = _
  private var latestCompletedTimeframe: Long = 0
  private val windowPerPost: util.HashMap[String, Window] = new util.HashMap[String, Window]()
  private val MIN_IN_MS = 60 * 1000

  private var slot: Int = 0

  private def withTumblingWindow(duration: BaseWindowedBolt.Duration) = {
    if (duration.value <= 0) throw new IllegalArgumentException("Window length must be positive [" + duration + "]")
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_SIZE_MS, duration)
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration)
    slot = 1
    this
  }

  private def withSlidingWindow(size: BaseWindowedBolt.Duration, slide: BaseWindowedBolt.Duration) = {
    if (size.value <= 0) throw new IllegalArgumentException("Window slide must be positive [" + size + "]")
    if (slide.value < size.value) throw new IllegalArgumentException("Window slide must be less than [" + size + "]")
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, slide)
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_SIZE_MS, size)
    val nSlot = size.value / slide.value
    if (nSlot == 0) throw new IllegalArgumentException("Window slide must be multiple of [" + size + "]")
    else slot = nSlot
    this
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
  }

  def handleMetronomeMessage(tuple: Tuple): Unit = {
    val ts: Long = tuple.getStringByField("ts").toLong

    val windowSlide: Long = windowConfiguration(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS).value.toLong // slide interval in ms
    val latestTimeFrame: Long = roundToCompletedMinute(ts)
    val elapsed: Long = latestTimeFrame - latestCompletedTimeframe // elapsed time from last frame in ms

    if (elapsed > windowSlide) {

      val frameToSlide = (elapsed / windowSlide).toInt // forward window of fromToSlide slot
      val expired = new util.ArrayList[String]()

      for (postID: String <- windowPerPost.keySet().asScala) {
        val w: Window = windowPerPost.get(postID)
        if (w != null) {

          w.moveForward(frameToSlide)
          val count: String = w.estimateTotal().toString
          if (w.estimateTotal() == 0) {
            expired.add(postID)
          }

          val values: Values = new Values()
          values.add(ts.toString)
          values.add(postID)
          values.add(count)
          values.add(latestTimeFrame.toString)


          _collector.emit(values)
        }

        // Free memory
        val iterator = expired.iterator()
        while (iterator.hasNext) {
          val elem = iterator.next()
          windowPerPost.remove(elem)
        }

        latestCompletedTimeframe = latestTimeFrame
      }
    }

    _collector.ack(tuple)
  }

  def handlePostTuple(tuple: Tuple): Unit = {
    val ts: Long = tuple.getStringByField("ts").toLong
    val id: String = tuple.getStringByField("post_commented")

    val slotSize: Int = windowConfiguration(Config.TOPOLOGY_BOLTS_WINDOW_SIZE_MS).value /
      windowConfiguration(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS).value

    val latestTimeFrame: Long = roundToCompletedMinute(ts)

    var w: Window = windowPerPost.get(id)
    if (w == null) {
      w = new Window(slot)
      windowPerPost.put(id, w)
    }
    w.increment()

    val count: String = w.estimateTotal().toString

    val values: Values = new Values()
    values.add(ts.toString)
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
