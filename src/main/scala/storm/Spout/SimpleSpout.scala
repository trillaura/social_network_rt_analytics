package storm.Spout

import java.util

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}

import scala.io.Source

class SimpleSpout extends BaseRichSpout {

  var _collector: SpoutOutputCollector = _
  var filename = "dataset/comments.dat"

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    _collector = collector
  }

  override def nextTuple(): Unit = {
    for (line <- Source.fromFile(filename).getLines()) {
      _collector.emit(new Values(line))
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("line"))
  }
}
