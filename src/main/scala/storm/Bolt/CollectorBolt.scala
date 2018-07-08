package storm.Bolt

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import utils.{Configuration, Parser}
import utils.kafka.{KafkaAvroParser, StormResultsProducer}
import utils.ranking.RankElement


/**
  * The collector receives the output stream from the GlobalRank bolt and
  * print them into the output source.
  */
class CollectorBolt extends BaseRichBolt {

  var _collector: OutputCollector = _

  var producer: StormResultsProducer = _

  override def execute(input: Tuple): Unit = {

    val ranking = input.getValueByField("globalRanking").asInstanceOf[List[RankElement[String]]]
    val start = input.getStringByField("timestamp")
    val rankElements = ranking.toArray

    if (Configuration.DEBUG) { println(start + " " + rankElements.mkString(" ")) }

    val timestamp = Parser.convertToDateTime(start).getMillis

    val data = KafkaAvroParser.fromCommentsResultsRecordToByteArray(
      timestamp, rankElements, KafkaAvroParser.schemaCommentResultsH1)

    val values = new Values()
    values.add(timestamp.toString)
    values.add(data)

    _collector.emit(values)
    _collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("timestamp", "globalRanking"))
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
  }

}
