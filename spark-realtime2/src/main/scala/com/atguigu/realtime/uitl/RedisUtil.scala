package com.atguigu.realtime.uitl

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
    private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, "hadoop201", 6379)
    
    // 直接得到一个 Redis 的连接
    def getJedisClient: Jedis = {
        jedisPool.getResource
    }
}
