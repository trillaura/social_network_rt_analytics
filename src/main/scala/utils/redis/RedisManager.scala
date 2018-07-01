package utils.redis

import com.redis.{RedisClient, RedisClientPool}
import utils.Configuration

object RedisManager {

  private val redisClient: RedisClient = new RedisClient(Configuration.REDIS_HOST, Configuration.REDIS_PORT)
  private val redisClientPool: RedisClientPool = new RedisClientPool(Configuration.REDIS_HOST, Configuration.REDIS_PORT)

  def getDefaultRedisClient : RedisClient = this.redisClient

  def getDefaultRedisClientPool : RedisClientPool = this.redisClientPool

}
