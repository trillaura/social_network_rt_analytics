package storm.Spout

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import utils.Configuration
import utils.kafka.{ConsumerManager, KafkaAvroParser, StormDataConsumer}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

/**
  * The Spout read from a Apache Kafka topic record in Avro format and forward them into the system
  */
class SimpleSpout extends BaseRichSpout {

  var _collector: SpoutOutputCollector = _

  var consumer: StormDataConsumer = _

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {

    consumer = new StormDataConsumer(1, Configuration.COMMENTS_INPUT_TOPIC)
    _collector = collector
  }

  override def nextTuple(): Unit = {

    val records = consumer.consume()

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
}


