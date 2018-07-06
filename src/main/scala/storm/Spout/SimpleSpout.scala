package storm.Spout

import java.util

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import utils.Configuration
import utils.kafka.{ConsumerManager, KafkaAvroParser}

import scala.collection.JavaConverters._
import scala.io.Source

class SimpleSpout extends BaseRichSpout {

  var _collector: SpoutOutputCollector = _
  var filename = "dataset/comments.dat"

  var consumer: Consumer[String, Array[Byte]] = _

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {

    prepareConsumer()
    _collector = collector
  }

  override def nextTuple(): Unit = {
    for (line <- Source.fromFile(filename).getLines()) {
      _collector.emit(new Values(line))
    }

    //    while (true) {

//    val records =
//      consumer.poll(1000)
//
//    records.asScala.foreach(
//      record =>
//        _collector.emit(new Values(record), record.timestamp())
//    )
    //    }

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("line"))
  }

  override def close(): Unit = {
    consumer.close()
  }


  def prepareConsumer(): Unit = {
    consumer = ConsumerManager.getDefaultConsumerStringByteArray
    ConsumerManager.subscribeConsumerStringByteArrayToTopic(Configuration.COMMENTS_INPUT_TOPIC)
  }
}
