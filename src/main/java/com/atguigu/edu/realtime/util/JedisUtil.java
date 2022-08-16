package com.atguigu.edu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * description:
 * Created by 铁盾 on 2022/6/14
 */
public class JedisUtil {
    private static JedisPool jedisPool;

    private static void initJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop103", 6379, 10000);
    }

    public static Jedis getJedis() {
        if (jedisPool == null) {
            synchronized (JedisUtil.class) {
                if (jedisPool == null) {
                    initJedisPool();
                }
            }
        }
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
}
