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

//    val record : GenericRecord =
//      KafkaAvroParser.fromByteArrayToCommentRecord(input.getValueByField("line").asInstanceOf[Array[Byte]])
//
//    val values = new Values()
//    values.add(record.get("ts").asInstanceOf[scala.Long])
//    values.add(record.get("comment_id").asInstanceOf[scala.Long])
//    values.add(record.get("user_id"))
//    values.add(record.get("comment"))
//    values.add(record.get("user"))
//    values.add(record.get("comment_replied"))
//    values.add(record.get("post_commented"))
//
//    _collector.emit(values)
//    _collector.ack(input)

    val line: String = input.getStringByField("line")

    val str: Array[String] = line.split("\\|")

    val value = new Values()
    value.add(str(0)) // timestamp
    value.add(str(1)) // comment id
    value.add(str(2)) // user id
    value.add(str(3)) // comment
    value.add(str(4)) // user

    // If split function returns 6 fields the post_commented is empty
    // and this is a comment of comment
    if (str.length == 6) {
      value.add(str(5)) // comment_replied
      value.add("") // post_commented
    } else {
      value.add("")
      value.add(str(6))
    }

    _collector.emit(value)
    _collector.ack(input)
  }
}
