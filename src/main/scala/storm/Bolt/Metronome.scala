package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.Configuration

/**
  * Metronome is in charge of beat time in order to communicate the following bolt that is time to emit.
  * Emission frequency is based on the timestamp field in the input record and  the slide interval of the following
  * windowed bolt. If the next bolt has a slide interval of one hour, the metronome emit every hour of event time.
  */
object Metronome extends BaseRichBolt {
  private var _collector: OutputCollector = _
  private var currentTime: Long = 0

  var S_METRONOME_HOURLY = "sMetronome.hourly"
  var S_METRONOME_DAiLY = "sMetronome.daily"
  var S_METRONOME_WEEKLY = "sMetronome.weekly"


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream(S_METRONOME_HOURLY, new Fields("ts", "post_commented", "count"))
    declarer.declareStream(S_METRONOME_DAiLY, new Fields("ts", "post_commented", "count"))
    declarer.declareStream(S_METRONOME_WEEKLY, new Fields("ts", "post_commented", "count"))
    declarer.declare(new Fields("ts", "post_commented", "count"))

  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
    val ts: String = input.getStringByField("ts")
    val postID: String = input.getStringByField("post_commented")

    val elapsed = ts.toLong - currentTime

    val values = new Values()
    values.add(ts)
    values.add(postID)
    values.add("1")

    if (elapsed > 0) {
      // Time must go forward
      if (elapsed > Configuration.hourlyCountWindowSlide) {
        _collector.emit(S_METRONOME_HOURLY, values)
      }

      if (elapsed > Configuration.dailyCountWindowSlide) {
        _collector.emit(S_METRONOME_DAiLY, values)
      }

      if (elapsed > Configuration.weeklyCountWindowSlide) {
        _collector.emit(S_METRONOME_WEEKLY, values)
      }

    }

    currentTime = ts.toLong
    _collector.ack(input)

  }
}
