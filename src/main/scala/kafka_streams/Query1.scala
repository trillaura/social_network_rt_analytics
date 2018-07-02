package kafka_streams

import java.util
import java.util.Map
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.lightbend.kafka.scala.streams._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Printed, Serialized, _}
import org.apache.kafka.streams.state.{StoreBuilder, Stores}
import utils.kafka.KafkaAvroParser
import utils.{Configuration, Parser, SerializerAny}


object Query1 {

  private val longSerde = Serdes.Long.asInstanceOf[Serde[scala.Long]]
  private val intSerde = Serdes.Integer.asInstanceOf[Serde[Int]]

  val DEBUG = true
    /*
 * 1) Create the input and output topics used by this example.
 * $ bin/kafka-topics.sh --create --topic wordcount-input --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics.sh  --create --topic streams-wordcount-output --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
 *
 * 2) Start this  application
 *
 * 3) Write some input data to the source topic "streams-plaintext-input"
 *      e.g.:
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic wordcount-input
 *
 * 4) Inspect the resulting data in the output topic "streams-wordcount-output"
 *      e.g.:
 * $ bin/kafka-console-consumer --topic streams-wordcount-output --from-beginning --new-consumer --bootstrap-server kafka0:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 */

  def createAvroStreamProperties(): Properties = {
    val props: Properties = new Properties()

    // Give the Streams application a unique name.
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, Configuration.APP_ID)
    props.put(StreamsConfig.CLIENT_ID_CONFIG, Configuration.CLIENT_ID)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    // Specify default (de)serializers for record keys and for record values.
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      Serdes.ByteArray().getClass.getName)

//    props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")

    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[EventTimestampExtractor])

    // Records should be flushed every 10 seconds.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    props
  }

  def execute(): Unit = {

    val props: Properties = createAvroStreamProperties()
    val builder: StreamsBuilderS = new StreamsBuilderS()

    val supplier = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(Configuration.STATE_STORE_NAME), Serdes.String, Serdes.ByteArray())
      .withLoggingEnabled(new util.HashMap[String,String]())

    builder.addStateStore(supplier)

    // Construct a `KStream` from the input topic
    val records_original: KStreamS[String, Array[Byte]] =
      builder
        .stream[String, Array[Byte]](Configuration.FRIENDS_INPUT_TOPIC)(Consumed.`with`(Serdes.String, Serdes.ByteArray))

    val records_filtered: KStreamS[String, Int] =
      records_original
        .flatMap(
          (_, v) => {
            val r = KafkaAvroParser.fromByteArrayToFriendshipRecord(v)
            val key = Parser.composeUserIDs(r)
            val hour = Parser.getHour(r.get("ts"))

            Iterable((key, hour))
          }
        )
        .groupByKey(Serialized.`with`(Serdes.String, intSerde))
        .reduce(
          (v1, v2) => {
            if (DEBUG) { println("Ignoring double friendships! "+ v1 + v2) }
            v1
          }
        )
        .toStream

    val resultsH24 = records_filtered
      .flatMap(
        (_, v) => {
          val hour = v
          Iterable((hour, 1l))
        }
      )
      .groupByKey(Serialized.`with`(intSerde, longSerde))
      .windowedBy(TimeWindows.of(24*60*60*1000))
      .reduce(_+_)
      .toStream

      .flatMap(
        (k, v) => {
          val res = Array.fill(24)(0l)
          val hour_index = k.key()
          res(hour_index) = v
          Iterable((k.window().start(), SerializerAny.serialize(res)))
        }
      )
      .groupByKey(Serialized.`with`(longSerde, Serdes.ByteArray()))
      .reduce(
        (v1, v2) => {
          val value1 = SerializerAny.deserialize(v1).asInstanceOf[Array[scala.Long]]
          if (DEBUG) {
            println("value1 ")
            value1.foreach(x => printf("%d.",x))
            printf("\n")
          }
          val value2 = SerializerAny.deserialize(v2).asInstanceOf[Array[scala.Long]]

          if (DEBUG) {
            println("value2")
            value1.foreach(x => printf("%d.",x))
            printf("\n")
          }

          val res = value1.zip(value2).map{case (x,y) => x+y}

          if (DEBUG) {
            println("res")
            res.foreach(x => printf("%d,",x))
            printf("\n")
          }
          SerializerAny.serialize(res)
        }
      )
      .toStream


    val resultsD7 =
      resultsH24
        .groupByKey(Serialized.`with`(longSerde, Serdes.ByteArray))
        .windowedBy(TimeWindows.of(7*24*60*60*1000))
        .reduce(
          (v1, v2) => {
            val value1 = SerializerAny.deserialize(v1).asInstanceOf[Array[scala.Long]]
            if (DEBUG) {
              println("value1 ")
              value1.foreach(x => printf("%d.",x))
              printf("\n")
            }
            val value2 = SerializerAny.deserialize(v2).asInstanceOf[Array[scala.Long]]

            if (DEBUG) {
              println("value2")
              value1.foreach(x => printf("%d.",x))
              printf("\n")
            }

            val res = value1.zip(value2).map{case (x,y) => x+y}

            if (DEBUG) {
              println("res")
              res.foreach(x => printf("%d,",x))
              printf("\n")
            }
            SerializerAny.serialize(res)
          }
        )
        .toStream
          .selectKey(
            (k, _) => k.key()
          )

//    results7D
    val resultsAllTime = resultsD7
        .selectKey(
          (_,_) => Configuration.STATE_STORE_NAME
        )
      .transform(
        () => new FromBeginningCountersProcessor(100), Configuration.STATE_STORE_NAME
    )


    if (DEBUG) {
      resultsH24.flatMap(
        (k, v) => {
          val value = SerializerAny.deserialize(v).asInstanceOf[Array[scala.Long]]
          printf("ts %d (", k)
          value.foreach(x => printf("%d,", x))
          printf(")\n")
          Iterable()
        }
      )
      resultsD7.flatMap(
        (k, v) => {
          val value = SerializerAny.deserialize(v).asInstanceOf[Array[scala.Long]]
          printf("ts %s (", k)
          value.foreach(x => printf("%d,", x))
          printf(")\n")
          Iterable()
        }
      )
    }

    if (DEBUG) { resultsH24.print(Printed.toSysOut[scala.Long, Array[Byte]]) }

    //         Write the `KTable<String, Long>` to the output topic.
    resultsH24.to(Configuration.FRIENDS_OUTPUT_TOPIC_H24)(Produced.`with`(longSerde, Serdes.ByteArray()))
    resultsD7.to(Configuration.FRIENDS_OUTPUT_TOPIC_D7)(Produced.`with`(longSerde, Serdes.ByteArray()))
    resultsAllTime.to(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME)(Produced.`with`(Serdes.String, Serdes.ByteArray()))

    // Now that we have finished the definition of the processing topology we can actually run
    // it via `start()`.  The Streams application as a whole can be launched just like any
    // normal Java application that has a `main()` method.
    val topology: Topology = builder.build()


    if (DEBUG) { println(topology.describe)}

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    val latch: CountDownLatch = new CountDownLatch(1)

    streams.cleanUp()
    streams.start()
    latch.await()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread {
      streams.close(10, TimeUnit.SECONDS)
      latch.countDown()
    })
  }

  def main(args: Array[String]): Unit = {
    execute()
  }
}