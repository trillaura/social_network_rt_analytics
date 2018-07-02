package utils.redis

import com.redis.{RedisClient, RedisClientPool}
import org.apache.avro.generic.GenericRecord
import utils.Configuration

object RedisManager {

  private val redisClient: RedisClient = new RedisClient(Configuration.REDIS_HOST, Configuration.REDIS_PORT)
  private val redisClientPool: RedisClientPool = new RedisClientPool(Configuration.REDIS_HOST, Configuration.REDIS_PORT)

  def getDefaultRedisClient : RedisClient = this.redisClient

  def getDefaultRedisClientPool : RedisClientPool = this.redisClientPool

  def writeAsyncInSortedSet(r: GenericRecord, name: String) : Unit = {

    val value = r.toString

//    this.redisClient.set()
  }
}
