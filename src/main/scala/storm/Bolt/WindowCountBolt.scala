package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import org.joda.time.{DateTime, DateTimeZone}
import utils.{Configuration, Window}

import scala.collection.JavaConverters._
import scala.collection.mutable

class WindowCountBolt extends BaseRichBolt {

  private val windowConfiguration: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()

  private var windowPerPost: util.HashMap[String, Window] = _

  private var windowStart: Long = 0

  private var nSlot: Int = _
  private var _collector: OutputCollector = _


  def withTumblingWindow(duration: Long) = {
    withSlidingWindow(duration, duration)
  }

  def withSlidingWindow(size: Long, slide: Long) = {
    if (size <= 0) throw new IllegalArgumentException("Window slide must be positive [" + size + "]")
    if (size < slide) throw new IllegalArgumentException("Window slide must be less than [" + size + "]")

    windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, slide)
    windowConfiguration.put(Configuration.TOPOLOGY_BOLTS_WINDOW_SIZE_MS, size)

    val slot = (size / slide).toInt
    if (slot == 0) throw new IllegalArgumentException("Window slide must be multiple of [" + size + "]")
    else nSlot = slot
    this
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    windowPerPost = new util.HashMap[String, Window]()
    _collector = collector
  }


  override def execute(input: Tuple): Unit = {

    val source = input.getSourceStreamId
    if (source.equals(Metronome.S_METRONOME_HOURLY) &&
      windowConfiguration(Configuration.TOPOLOGY_BOLTS_WINDOW_SIZE_MS) == Configuration.hourlyCountWindowSize) {
      handleMetronomeMessage(input)
    } else if (source.equals(Metronome.S_METRONOME_DAiLY) &&
      windowConfiguration(Configuration.TOPOLOGY_BOLTS_WINDOW_SIZE_MS) == Configuration.dailyCountWindowSize) {
      handleMetronomeMessage(input)
    } else if (source.equals(Metronome.S_METRONOME_WEEKLY) &&
      windowConfiguration(Configuration.TOPOLOGY_BOLTS_WINDOW_SIZE_MS) == Configuration.weeklyCountWindowSize) {
      handleMetronomeMessage(input)
    } else {
      handlePostTuple(input)
    }

  }

  def handleMetronomeMessage(tuple: Tuple): Unit = {
    val currentTime: Long = tuple.getStringByField("ts").toLong
    val windowSlide: Long = windowConfiguration(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS) // slide interval in ms

    val elapsed: Long = currentTime - windowStart // elapsed time from last frame in ms
    val frameToSlide = (elapsed / windowSlide).toInt // forward window of fromToSlide nslot

    if (frameToSlide > 0) {
      windowStart += (frameToSlide * windowSlide)

      val expired = new util.ArrayList[String]()

      for (postID: String <- windowPerPost.keySet().asScala) {
        val w: Window = windowPerPost.get(postID)

        val estimateTotal: Int = w.estimateTotal()

        w.moveForward(frameToSlide)

        if (estimateTotal != 0) {
          expired.add(postID)


          val values: Values = new Values()
          values.add(currentTime.toString)
          values.add(postID)
          values.add(estimateTotal.toString)
          values.add(windowStart.toString)

          _collector.emit(values)
        }
      }

      // Free memory
      val iterator = expired.iterator()
      while (iterator.hasNext) {
        val elem = iterator.next()
        windowPerPost.remove(elem)
      }
    }

    _collector.ack(tuple)
  }

  def handlePostTuple(tuple: Tuple): Unit = {
    val ts: Long = tuple.getStringByField("ts").toLong
    val id: String = tuple.getStringByField("post_commented")
    val count: Int = tuple.getStringByField("count").toInt

    var w: Window = windowPerPost.get(id)
    if (w == null) {
      w = new Window(nSlot)
      windowPerPost.put(id, w)
    }

    w.increment(count)

    _collector.ack(tuple)
  }


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("ts", "post_commented", "count", "start"))
  }

  def isValid(timestamp: Long): Boolean = {
    val date = new DateTime(timestamp).withZone(DateTimeZone.UTC)
    val windowLength = windowConfiguration(Configuration.TOPOLOGY_BOLTS_WINDOW_SIZE_MS)

    if ((windowLength == Configuration.dailyCountWindowSize && date.getMinuteOfHour == 0) || (windowLength == Configuration.weeklyCountWindowSize && date.getHourOfDay == 0))
      if (windowLength == Configuration.weeklyCountWindowSize)
        return true


    false
  }
}
