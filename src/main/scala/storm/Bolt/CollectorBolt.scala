package storm.Bolt

import java.util

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import utils.{Configuration, ResultsFileWriter}
import utils.kafka.{KafkaAvroParser, ProducerManager}
import utils.ranking.{RankElement, RankingResult}

class CollectorBolt extends BaseRichBolt {

  var _collector: OutputCollector = _

  var producer: Producer[Long, Array[Byte]] = _
  var gson: Gson = _

  override def execute(input: Tuple): Unit = {

    val ranking = input.getValueByField("globalRanking").asInstanceOf[List[RankElement[String]]]
    val start = input.getStringByField("timestamp")
    val rankElements = ranking.toArray

    ResultsFileWriter.writeLine(start + " " + rankElements.mkString(" "), "storm")
//    println(start + " " + rankElements.mkString(" "))

    //    val timestamp = input.getLongByField("ts").asInstanceOf[scala.Long]
    //    val post_id_1 = input.getLongByField("post_id_1").asInstanceOf[scala.Long]
    //    val num_comments_1 = input.getLongByField("num_comments_1").asInstanceOf[scala.Long]
    //    val post_id_2 = input.getLongByField("post_id_2").asInstanceOf[scala.Long]
    //    val num_comments_2 = input.getLongByField("num_comments_2").asInstanceOf[scala.Long]
    //    val post_id_3 = input.getLongByField("post_id_3").asInstanceOf[scala.Long]
    //    val num_comments_3 = input.getLongByField("num_comments_3").asInstanceOf[scala.Long]
    //    val post_id_4 = input.getLongByField("post_id_4").asInstanceOf[scala.Long]
    //    val num_comments_4 = input.getLongByField("num_comments_4").asInstanceOf[scala.Long]
    //    val post_id_5 = input.getLongByField("post_id_5").asInstanceOf[scala.Long]
    //    val num_comments_5 = input.getLongByField("num_comments_5").asInstanceOf[scala.Long]
    //    val post_id_6 = input.getLongByField("post_id_6").asInstanceOf[scala.Long]
    //    val num_comments_6 = input.getLongByField("num_comments_6").asInstanceOf[scala.Long]
    //    val post_id_7 = input.getLongByField("post_id_7").asInstanceOf[scala.Long]
    //    val num_comments_7 = input.getLongByField("num_comments_7").asInstanceOf[scala.Long]
    //    val post_id_8 = input.getLongByField("post_id_8").asInstanceOf[scala.Long]
    //    val num_comments_8 = input.getLongByField("num_comments_8").asInstanceOf[scala.Long]
    //    val post_id_9 = input.getLongByField("post_id_9").asInstanceOf[scala.Long]
    //    val num_comments_9 = input.getLongByField("num_comments_9").asInstanceOf[scala.Long]
    //    val post_id_10 = input.getLongByField("post_id_10").asInstanceOf[scala.Long]
    //    val num_comments_10 = input.getLongByField("num_comments_10").asInstanceOf[scala.Long]
    //
    //    val data = KafkaAvroParser.fromCommentsResultsRecordToByteArray(
    //      timestamp, post_id_1, num_comments_1, post_id_2, num_comments_2,
    //      post_id_3, num_comments_3, post_id_4, num_comments_4, post_id_5,
    //      num_comments_5, post_id_6, num_comments_6, post_id_7, num_comments_7,
    //      post_id_8, num_comments_8, post_id_9, num_comments_9, post_id_10, num_comments_10,
    //      KafkaAvroParser.schemaCommentResultsH1)
    //
    //    val record: ProducerRecord[Long, Array[Byte]] =
    //      new ProducerRecord(Configuration.COMMENTS_OUTPUT_TOPIC_H1, timestamp, data)
    //
    //    val p: Thread = new Thread {
    //      producer.send(record)
    //    }
    //    p.start()

    _collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // nothing to declare
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    gson = new Gson()
    producer = ProducerManager.getDefaultProducer
  }

}
