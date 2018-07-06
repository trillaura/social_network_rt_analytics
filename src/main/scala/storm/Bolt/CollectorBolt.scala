package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import utils.{Configuration, Parser, ResultsFileWriter}
import utils.kafka.{KafkaAvroParser, ProducerManager}
import utils.ranking.{RankElement, RankingResult}

class CollectorBolt extends BaseRichBolt {

  var _collector: OutputCollector = _

  var producer: Producer[Long, Array[Byte]] = _

  override def execute(input: Tuple): Unit = {

    val ranking = input.getValueByField("globalRanking").asInstanceOf[List[RankElement[String]]]
    val start = input.getStringByField("timestamp")
    val rankElements = ranking.toArray



//    ResultsFileWriter.writeLine(start + " " + rankElements.mkString(" "), "storm")
//    println(start + " " + rankElements.mkString(" "))

    val timestamp = Parser.convertToDateTime(start).getMillis
//    val post_id_1 = rankElements(0).id.toLong
//    val num_comments_1 = rankElements(0).score.toLong
//    val post_id_2 = rankElements(1).id.toLong
//    val num_comments_2 = rankElements(1).score.toLong
//    val post_id_3 = rankElements(2).id.toLong
//    val num_comments_3 = rankElements(2).score.toLong
//    val post_id_4 = rankElements(3).id.toLong
//    val num_comments_4 = rankElements(3).score.toLong
//    val post_id_5 = rankElements(4).id.toLong
//    val num_comments_5 = rankElements(4).score.toLong
//    val post_id_6 = rankElements(5).id.toLong
//    val num_comments_6 = rankElements(5).score.toLong
//    val post_id_7 = rankElements(6).id.toLong
//    val num_comments_7 = rankElements(6).score.toLong
//    val post_id_8 = rankElements(7).id.toLong
//    val num_comments_8 = rankElements(7).score.toLong
//    val post_id_9 = rankElements(8).id.toLong
//    val num_comments_9 = rankElements(8).score.toLong
//    val post_id_10 = rankElements(9).id.toLong
//    val num_comments_10 = rankElements(9).score.toLong

//    val data = KafkaAvroParser.fromCommentsResultsRecordToByteArray(
//      timestamp, post_id_1, num_comments_1, post_id_2, num_comments_2,
//      post_id_3, num_comments_3, post_id_4, num_comments_4, post_id_5,
//      num_comments_5, post_id_6, num_comments_6, post_id_7, num_comments_7,
//      post_id_8, num_comments_8, post_id_9, num_comments_9, post_id_10, num_comments_10,
//      KafkaAvroParser.schemaCommentResultsH1)

    val data = KafkaAvroParser.fromCommentsResultsRecordToByteArray(
      timestamp, rankElements, KafkaAvroParser.schemaCommentResultsH1)

    val record: ProducerRecord[Long, Array[Byte]] =
      new ProducerRecord(Configuration.COMMENTS_OUTPUT_TOPIC_H1, timestamp, data)

    val p: Thread = new Thread {
      producer.send(record)
    }
    p.start()

    _collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // nothing to declare
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    producer = ProducerManager.getDefaultProducer
  }

}
