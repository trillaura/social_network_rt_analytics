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

import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable

class WindowCountBolt extends BaseRichBolt {

  @transient
  private lazy val windowConfiguration: mutable.Map[String, Duration] = new mutable.HashMap[String, Duration]()

  private val windowPerPost: util.HashMap[String, Window] = new util.HashMap[String, Window]()

  private var windowStart: Long = 0

  private var nSlot: Int = _
  private var _collector: OutputCollector = _


  private def withTumblingWindow(duration: BaseWindowedBolt.Duration) = {
    withSlidingWindow(duration, duration)
  }

  private def withSlidingWindow(size: BaseWindowedBolt.Duration, slide: BaseWindowedBolt.Duration) = {
    if (size.value <= 0) throw new IllegalArgumentException("Window slide must be positive [" + size + "]")
    if (slide.value < size.value) throw new IllegalArgumentException("Window slide must be less than [" + size + "]")

    windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, slide)
    windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_SIZE_MS, size)

    val slot = size.value / slide.value
    if (slot == 0) throw new IllegalArgumentException("Window slide must be multiple of [" + size + "]")
    else nSlot = slot
    this
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector


  override def execute(input: Tuple): Unit = {

    if (input.getSourceStreamId.equals("Metronome")) {
      handleMetronomeMessage(input)
    } else {
      handlePostTuple(input)
    }

  }

  def handleMetronomeMessage(tuple: Tuple): Unit = {
    val ts: Long = tuple.getStringByField("ts").toLong

    val windowSlide: Long = windowConfiguration(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS).value.toLong // slide interval in ms
    val currentTime: Long = roundToCompletedMinute(ts)

    val elapsed: Long = currentTime - windowStart // elapsed time from last frame in ms
    val frameToSlide = (elapsed / windowSlide).toInt // forward window of fromToSlide nslot

    if (frameToSlide > 0) {

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
          values.add(windowSlide)


          _collector.emit(values)
        }

        // Free memory
        val iterator = expired.iterator()
        while (iterator.hasNext) {
          val elem = iterator.next()
          windowPerPost.remove(elem)
        }

        windowStart += frameToSlide * windowSlide
      }
    }

    _collector.ack(tuple)
  }

  def handlePostTuple(tuple: Tuple): Unit = {
    val ts: Long = tuple.getStringByField("ts").toLong
    val id: String = tuple.getStringByField("post_commented")
    val count: Int = tuple.getStringByField("count").toInt

    val windowSlide: Long = windowConfiguration(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS).value.toLong // slide interval in ms
    val currentTime: Long = roundToCompletedMinute(ts)

    val elapsed: Long = currentTime - windowStart // elapsed time from last frame in ms
    val frameToSlide = (elapsed / windowSlide).toInt // forward window of fromToSlide nslot

    if (frameToSlide > 0) {
      val expired = new util.ArrayList[String]()

      for (postID: String <- windowPerPost.keySet().asScala) {
        val w: Window = windowPerPost.get(postID)
        if (w != null) {

          w.moveForward(frameToSlide)

          if (postID == id) {
            val count: String = w.estimateTotal().toString
            if (w.estimateTotal() == 0) {
              expired.add(postID)
            }
            val values: Values = new Values()
            values.add(ts.toString)
            values.add(postID)
            values.add(count)
            values.add(windowSlide)

            _collector.emit(values)
          }
        }
      }

      // Free memory
      val iterator = expired.iterator()
      while (iterator.hasNext) {
        val elem = iterator.next()
        windowPerPost.remove(elem)
      }

      windowStart += frameToSlide * windowSlide

    }


    var w: Window = windowPerPost.get(id)
    if (w == null) {
      w = new Window(nSlot)
      windowPerPost.put(id, w)
    }

    if (tuple.contains("start")) {
      val start: Long = tuple.getStringByField("start").toLong
      if (isValid(start)) {
        w.increment(count)
      }
    } else {
      w.increment()
    }
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

  def isValid(timestamp: Long): Boolean = {
    val date = new DateTime(timestamp).withZone(DateTimeZone.UTC)
    val windowLength = windowConfiguration(Config.TOPOLOGY_BOLTS_WINDOW_SIZE_MS).value

    if (windowLength == Config.dailyCountWindowSize && date.getMinuteOfHour == 0 ||
      windowLength == Config.weeklyCountWindowSize && date.getHourOfDay == 0) return true


    false
  }
}
