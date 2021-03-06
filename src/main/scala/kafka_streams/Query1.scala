package kafka_streams

import java.util
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.lightbend.kafka.scala.streams._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Printed, Serialized, _}
import org.apache.kafka.streams.state.Stores
import org.joda.time.{DateTime, DateTimeZone}
import utils.kafka.KafkaAvroParser
import utils.{Configuration, Parser, SerializerAny}

/**
  * QUERY 1
  */
object Query1 {

  private val longSerde = Serdes.Long.asInstanceOf[Serde[scala.Long]]
  private val intSerde = Serdes.Integer.asInstanceOf[Serde[Int]]

  def createAvroStreamProperties(): Properties = {
    val props: Properties = new Properties()

    // Give the Streams application a unique name.
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, Configuration.APP_ID)
    props.put(StreamsConfig.CLIENT_ID_CONFIG, Configuration.CLIENT_ID)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS)
    // Specify default (de)serializers for record keys and for record values.
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      Serdes.Long.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      Serdes.ByteArray.getClass.getName)
    // to collect all metrics
    props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[EventTimestampExtractor])

    // Records should be flushed every 10 seconds.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    props
  }

  def execute(): Unit = {

    val props: Properties = createAvroStreamProperties()
    val builder: StreamsBuilderS = new StreamsBuilderS()

    val topic_config = new util.HashMap[String,String]()
    val supplier = Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore(Configuration.STATE_STORE_NAME), Serdes.String, Serdes.ByteArray())
      .withLoggingEnabled(topic_config)

    builder.addStateStore(supplier)

    // Construct a `KStream` from the input topic
    val records_original: KStreamS[Long, Array[Byte]] =
      builder
        .stream[Long, Array[Byte]](Configuration.FRIENDS_INPUT_TOPIC)(Consumed.`with`(longSerde, Serdes.ByteArray))

    // Filtering friendships referring to the same couple of users
    val records_filtered: KStreamS[String, Long] =
      records_original
        .map(
          (k: scala.Long, v: Array[Byte])
          => {
            val r = KafkaAvroParser.fromByteArrayToFriendshipRecord(v)
            val newKey = Parser.composeUserIDs(r)
            (newKey, k)
          }
        )
        .groupByKey(Serialized.`with`(Serdes.String, longSerde))
        .reduce(
          (timestamp, _) => {
            if (Configuration.DEBUG) { println("Ignoring double friendships! "+ timestamp) }
            timestamp
          }
        )
        .toStream

    // Compute daily statistics using Tumbling Window of 24 hour to group values to count
    val resultsH24 = records_filtered
      .map(
        (_, timestamp) => {
          (timestamp, 1l)
        }
      )
      .groupByKey(Serialized.`with`(longSerde, longSerde))
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(24)).advanceBy(TimeUnit.HOURS.toMillis(24)))
      .count("counter-by-h24-window")
      .toStream
      .map(
        (windowed_k, v) => {
          val init = windowed_k.window().start()
          val hour = Parser.convertToDateTime(windowed_k.key()).getHourOfDay
          val counter = v
          val value = Array.fill(24)(0l)
          value(hour) = counter
          (init, SerializerAny.serialize(value))
        }
      )
      .groupByKey(Serialized.`with`(longSerde, Serdes.ByteArray()))
      .reduce(
        (a,b) => {
          val value1 = SerializerAny.deserialize(a).asInstanceOf[Array[scala.Long]]
          val value2 = SerializerAny.deserialize(b).asInstanceOf[Array[scala.Long]]
          val newVal = value1.zip(value2).map{case (x,y) => x+y}
          SerializerAny.serialize(newVal)
        }
      )
      .toStream

    // Compute weekly statistics from the daily one grouping through a Sliding Window of 24 hours
    // returning updated results every day
    val resultsD7 = resultsH24
      .groupByKey(Serialized.`with`(longSerde, Serdes.ByteArray))
      .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(7)).advanceBy(TimeUnit.HOURS.toMillis(24)))
      .reduce(
        (a,b) => {
          val value1 = SerializerAny.deserialize(a).asInstanceOf[Array[scala.Long]]
          val value2 = SerializerAny.deserialize(b).asInstanceOf[Array[scala.Long]]
          val newVal = value1.zip(value2).map{case (x,y) => x+y}
          SerializerAny.serialize(newVal)
        }
      )
      .toStream
      .selectKey((windowed_k, _) => windowed_k.window().start)

    // Compute from beginning statistics from the weekly one apply a stream transformer to update
    // state store with statistics and related begin timestamp, returning updated results every day
    val resultsAllTime = resultsH24
      .map(
        (timestamp, values) => {
          val deser_values = SerializerAny.deserialize(values).asInstanceOf[Array[scala.Long]]
          val new_values = Array.fill(25)(0l)
          for (i <- new_values.indices) {
            if (i == 0)
              new_values(i) = timestamp
            else
              new_values(i) = deser_values(i-1)
          }
          (Configuration.STATE_STORE_NAME, SerializerAny.serialize(new_values))
        }
      )
      .transform(
        () => new FromBeginningCounterTransformer(100), Configuration.STATE_STORE_NAME
      )


    if (Configuration.DEBUG) {
      resultsH24.foreach(
        (x, y) => {
          val value = SerializerAny.deserialize(y).asInstanceOf[Array[Long]]
          printf("init --> %s --> ", new DateTime(x, DateTimeZone.UTC).toString(Parser.TIMESTAMP_FORMAT))
          value.foreach(d => printf("%d - ", d))
          printf("\n")
        }
      )

      resultsD7.foreach(
        (x, y) => {
          val value = SerializerAny.deserialize(y).asInstanceOf[Array[Long]]
          printf("initD7 --> %s --> ", new DateTime(x, DateTimeZone.UTC).toString(Parser.TIMESTAMP_FORMAT))
          value.foreach(d => printf("%d - ", d))
          printf("\n")
        }
      )

      resultsAllTime.foreach(
        (x, y) => {
          val value = SerializerAny.deserialize(y).asInstanceOf[Array[Long]]
          printf("initALLTIME --> %s --> ", new DateTime(value(0), DateTimeZone.UTC).toString(Parser.TIMESTAMP_FORMAT))
          value.foreach(d => printf("%d - ", d))
          printf("\n")
        }
      )
    }

    if (Configuration.DEBUG) { resultsH24.print(Printed.toSysOut[scala.Long, Array[Byte]]) }
    if (Configuration.DEBUG) { resultsD7.print(Printed.toSysOut[scala.Long, Array[Byte]]) }
    if (Configuration.DEBUG) { resultsAllTime.print(Printed.toSysOut[String, Array[Byte]]) }


    // Write  to the output topic.
    resultsH24
      .map(
        (timestamp, array) => {
          val values = SerializerAny.deserialize(array).asInstanceOf[Array[scala.Long]]
          (timestamp, KafkaAvroParser.fromFriendshipsResultsRecordToByteArray(timestamp, values, KafkaAvroParser.schemaFriendshipResultsH24))
        }
      )
      .to(Configuration.FRIENDS_OUTPUT_TOPIC_H24)(Produced.`with`(longSerde, Serdes.ByteArray()))

    resultsD7
      .map(
        (timestamp, array) => {
          val values = SerializerAny.deserialize(array).asInstanceOf[Array[scala.Long]]
          (timestamp, KafkaAvroParser.fromFriendshipsResultsRecordToByteArray(timestamp, values, KafkaAvroParser.schemaFriendshipResultsD7))
        }
      )
      .to(Configuration.FRIENDS_OUTPUT_TOPIC_D7)(Produced.`with`(longSerde, Serdes.ByteArray()))

    resultsAllTime
      .map(
        (state, array) => {
          val values = SerializerAny.deserialize(array).asInstanceOf[Array[scala.Long]]
          (state, KafkaAvroParser.fromFriendshipsResultsRecordToByteArray(0l, values, KafkaAvroParser.schemaFriendshipResultsAllTime))
        }
      )
      .to(Configuration.FRIENDS_OUTPUT_TOPIC_ALLTIME)(Produced.`with`(Serdes.String, Serdes.ByteArray()))

    val topology: Topology = builder.build()

    if (Configuration.DEBUG) { println(topology.describe)}

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
    val params : ParameterTool= ParameterTool.fromArgs(args)
    val bootstrapServers = params.get("bootstrap")
    val zookeeper = params.get("zookeeper", Configuration.ZOOKEEPER_SERVERS)

    Configuration.BOOTSTRAP_SERVERS = bootstrapServers
    Configuration.ZOOKEEPER_SERVERS = zookeeper

    execute()
  }
}