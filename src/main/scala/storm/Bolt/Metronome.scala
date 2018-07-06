package storm.Bolt

import java.util
import java.util.{Calendar, Date, GregorianCalendar}

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

class Metronome extends BaseRichBolt {
  private var _collector: OutputCollector = _
  private var currentTime: Long = 0

  var S_METRONOME = "sMetronome"


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream(S_METRONOME, new Fields("ts", "post_commented"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {
    /* Emit message every simulated minute */
    val ts: String = input.getStringByField("ts")
    val postID: String = input.getStringByField("post_commented")

    val time: Long = roundToCompletedMinute(ts.toLong)

    // Time must go forward
    if (currentTime < time) {
      currentTime = time

      val values = new Values()
      values.add(ts)
      values.add(postID)

      _collector.emit(S_METRONOME, values)

    } else {
      /* Do nothing. Time not go forward */
    }

    _collector.ack(input)

  }


  private def roundToCompletedMinute(timestamp: Long) = {

    val d = new Date(timestamp)
    val date = new GregorianCalendar
    date.setTime(d)
    date.set(Calendar.SECOND, 0)
    date.set(Calendar.MILLISECOND, 0)
    date.getTime.getTime
  }
}
