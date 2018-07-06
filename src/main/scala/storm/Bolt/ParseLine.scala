package storm.Bolt

import java.util

import org.apache.avro.generic.GenericRecord
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.kafka.KafkaAvroParser

class ParseLine extends BaseRichBolt {

  private var _collector: OutputCollector = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("ts", "comment id ", "user id", "comment", "user", "comment_replied", "post_commented"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit =
    _collector = collector

  override def execute(input: Tuple): Unit = {

    val record : GenericRecord =
      KafkaAvroParser.fromByteArrayToCommentRecord(input.getValueByField("line").asInstanceOf[Array[Byte]])

    val values = new Values()
    values.add(record.get("ts").toString)
    values.add(record.get("comment_id").toString)
    values.add(record.get("user_id").toString)
    values.add(record.get("comment").toString)
    values.add(record.get("user").toString)
    values.add(record.get("comment_replied").toString)
    values.add(record.get("post_commented").toString)

    _collector.emit(values)
    _collector.ack(input)
  }
}
