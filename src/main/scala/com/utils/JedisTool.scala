package com.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisTool {
  private val config = new GenericObjectPoolConfig()
  private val pool = new JedisPool(config,"hadoop101",6379)
  def getResource()=pool.getResource
}
