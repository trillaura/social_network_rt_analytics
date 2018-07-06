package utils.redis

import com.redis.{RedisClient, RedisClientPool}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import utils.{Configuration, ResultsFileWriter}
import utils.kafka.KafkaAvroParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Manager for Redis client operations for asynchronously writing and reading from
  * Redis storage data.
  */
object RedisManager {

  private val redisClient: RedisClient = new RedisClient(Configuration.REDIS_HOST, Configuration.REDIS_PORT)
  private val redisClientPool: RedisClientPool = new RedisClientPool(Configuration.REDIS_HOST, Configuration.REDIS_PORT)

  def getDefaultRedisClient : RedisClient = this.redisClient

  def getDefaultRedisClientPool : RedisClientPool = this.redisClientPool

  /**
    * Add string to a sorted set container stored at key (schema name of the string)
    * associating current timestamp as score and the string itself as value.
    * @param records: records consumed
    */
  def writeAsyncInSortedSet(records: ConsumerRecords[Long, Array[Byte]], topic: String) : Unit = {

    try {
      this.redisClientPool.withClient {
        client => {
          records.asScala.foreach( record => {
            val r: GenericRecord =
              KafkaAvroParser.fromByteArrayToRecord(record.value, topic)

            if (Configuration.DEBUG) { println("Consumed record with schema " + r.getSchema.getName) }

            client.zadd(r.getSchema.getName, System.currentTimeMillis(), r.toString)
          }
          )}
      }
    } catch {
      case _:Throwable => println("ERR: consuming record")
    }
  }

  /**
    * Read all data records stored in a sorted set keyed by schema
    * name.
    * @param name: schema name as key
    * @return
    */
  def getResultsBySchema(name: String) : List[String] = {

    val read = this.redisClient.zrange(name, 0, System.currentTimeMillis().toInt)

    if (read.isDefined) {
      val values = read.get
      println("Records with key: " + name)
      values.foreach(println(_))
      return values
    } else
      println("No records with key: " + name)
      List[String]()
  }

  def main(args: Array[String]): Unit = {
    while (true) {
      val res1 = getResultsBySchema(KafkaAvroParser.schemaFriendshipResultsH24.getName)
      val res2 = getResultsBySchema(KafkaAvroParser.schemaFriendshipResultsD7.getName)
      val res3 = getResultsBySchema(KafkaAvroParser.schemaFriendshipResultsAllTime.getName)
      val res4 = getResultsBySchema(KafkaAvroParser.schemaCommentResultsH1.getName)
      val res5 = getResultsBySchema(KafkaAvroParser.schemaCommentsResultsH24.getName)
      val res6 = getResultsBySchema(KafkaAvroParser.schemaCommentsResultsD7.getName)
      val res7 = getResultsBySchema(KafkaAvroParser.schemaPostsResultsH1.getName)
      val res8 = getResultsBySchema(KafkaAvroParser.schemaPostsResultsH24.getName)
      val res9 = getResultsBySchema(KafkaAvroParser.schemaPostsResultsD7.getName)

//      res1.foreach(line => ResultsFileWriter.writeLine(line, KafkaAvroParser.schemaFriendshipResultsH24.getName))
//      res2.foreach(line => ResultsFileWriter.writeLine(line, KafkaAvroParser.schemaFriendshipResultsD7.getName))
//      res3.foreach(line => ResultsFileWriter.writeLine(line, KafkaAvroParser.schemaFriendshipResultsAllTime.getName))

      Thread.sleep(10000)
    }
  }
}
