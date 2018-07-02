package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

class CollectorBolt extends BaseRichBolt {
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = ???

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = ???

  override def execute(input: Tuple): Unit = ???
}
