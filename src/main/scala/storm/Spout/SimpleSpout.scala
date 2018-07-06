package storm.Spout

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.Consumer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import utils.Configuration
import utils.kafka.ConsumerManager

import scala.collection.JavaConverters._

/**
  * The Spout read from a Apache Kafka topic record in Avro format and forward them into the system
  */
class SimpleSpout extends BaseRichSpout {

  var _collector: SpoutOutputCollector = _
  var consumer: Consumer[String, Array[Byte]] = _

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {

    prepareConsumer()
    _collector = collector
  }

  override def nextTuple(): Unit = {

    // read from kafka
    val records =
      consumer.poll(1000)

    records.asScala.foreach(
      record => {
        val objMapper = new ObjectMapper()
        _collector.emit(new Values(objMapper.writeValueAsBytes(record.value)), record.timestamp())
      }
    )
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("line"))
  }

  override def close(): Unit = {
    consumer.close()
  }


  def prepareConsumer(): Unit = {
    consumer = ConsumerManager.createInputConsumer(Configuration.COMMENTS_INPUT_TOPIC)
  }
}


