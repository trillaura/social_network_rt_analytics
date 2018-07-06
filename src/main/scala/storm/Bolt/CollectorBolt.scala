package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import utils.{Configuration, Parser, ResultsFileWriter}
import utils.kafka.{KafkaAvroParser, ProducerManager, StormResultsProducer}
import utils.ranking.{RankElement, RankingResult}


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



    ////    ResultsFileWriter.writeLine(start + " " + rankElements.mkString(" "), "storm")
    println(start + " " + rankElements.mkString(" "))

    val timestamp = Parser.convertToDateTime(start).getMillis

    val data = KafkaAvroParser.fromCommentsResultsRecordToByteArray(
      timestamp, rankElements, KafkaAvroParser.schemaCommentResultsH1)


    val p: Thread = new Thread {
      producer.produce(timestamp, data)
    }
    p.run()
    p.start()

    _collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // nothing to declare
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    producer = new StormResultsProducer(1, Configuration.COMMENTS_OUTPUT_TOPIC_H1)
  }

}
