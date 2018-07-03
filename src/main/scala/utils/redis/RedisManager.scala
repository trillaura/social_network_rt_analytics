package utils.redis

import com.redis.{RedisClient, RedisClientPool}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import utils.Configuration
import utils.kafka.KafkaAvroParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object RedisManager {

  private val redisClient: RedisClient = new RedisClient(Configuration.REDIS_HOST, Configuration.REDIS_PORT)
  private val redisClientPool: RedisClientPool = new RedisClientPool(Configuration.REDIS_HOST, Configuration.REDIS_PORT)

  def getDefaultRedisClient : RedisClient = this.redisClient

  def getDefaultRedisClientPool : RedisClientPool = this.redisClientPool

  /**
    * scala-redis is a blocking client for Redis. But you can develop high performance
    * asynchronous patterns of computation using scala-redis and Futures.
    * RedisClientPool allows you to work with multiple RedisClient instances and Futures
    * offer a non-blocking semantics on top of this. The combination can give you good
    * numbers for implementing common usage patterns like scatter/gather.
    * Here's an example that you will also find in the test suite. It uses the scatter/gather
    * technique to do loads of push across many lists in parallel. The gather phase pops from
    * all those lists in parallel and does some computation over them.
    */

  /**
    * Add string to a sorted set container stored at key (schema name of the string)
    * associating current timestamp as score and the string itself as value.
    * @param records: records consumed
    */
  def writeAsyncInSortedSet(records: ConsumerRecords[Long, Array[Byte]], topic: String) : Unit = {

    var values: ListBuffer[GenericRecord] = ListBuffer()
    records.asScala.foreach(record => {
      val r: GenericRecord =
        KafkaAvroParser.fromByteArrayToResultRecord(record.value, topic)
      values += r
    })

    try {
      this.redisClientPool.withClient {
        client => {
          values.foreach( r => {
            client.zadd(r.getSchema.getName, System.currentTimeMillis(), r.toString)
          }
          )}
      }
    } catch {
      case _:Throwable => println("err")
    }
  }
}
