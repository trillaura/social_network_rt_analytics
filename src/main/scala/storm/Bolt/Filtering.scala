package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

class Filtering extends BaseRichBolt {
  var _collector: OutputCollector = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("ts", "post_commented"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = _collector = collector

  override def execute(input: Tuple): Unit = {

    val postID = input.getStringByField("post_commented")

    // This is a comment of comment and not a comment to a post
    // so we discard it
    if (postID.isEmpty) {
      _collector.ack(input)
      return
    }
    val timestamp = input.getStringByField("ts")

    val values = new Values()
    values.add(timestamp)
    values.add(postID)

    _collector.emit(values)
    _collector.ack(input)
  }
}