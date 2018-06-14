package kafka_streams

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import kafka_streams.ProducerLauncher.{SLEEP, schema}
import org.apache.avro.generic.{GenericData, GenericRecord}

object Main {

  def executeWordCountKStreams() : Unit = {

    // process
//    val w: Thread = new Thread {
//      WordCount.execute()
//    }
//    w.start()

    // consume
    ConsumerLauncher.createAvroConsumer()
    val c: Thread = new Thread{
      override def run(): Unit ={
        ConsumerLauncher.consumeAvro()
      }
    }
    c.start()

    // produce
    ProducerLauncher.createAvroProducer()
    for (i <- 0 until 10) {
      val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("ts", System.currentTimeMillis())
      avroRecord.put("user_id1", System.currentTimeMillis() % 32)
      avroRecord.put("user_id2", System.currentTimeMillis() % 51)

      val bytes: Array[Byte] = recordInjection.apply(avroRecord)
      ProducerLauncher.produceAvro(bytes)
      Thread.sleep(SLEEP)
    }
    ProducerLauncher.closeAvro()
  }

  def main(args: Array[String]) : Unit = {

//    Parser.readFile("dataset/friendships.dat")

    executeWordCountKStreams()
  }
}
