package com.gallen.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by gallenvara on 16/12/3.
 */
public class RedisSink {
    public static void main(String[] args) {
        /*Jedis redis = new Jedis("127.0.0.1", 6379);
        //redis.auth("admin");

        redis.lpush("key", "gallen", "redis");
        redis.del("key");
        redis.set("key1", "1");
        System.out.println(redis.lrange("key", 0, -1));
        System.out.println(redis.get("key1"));*/

        Jedis pool = RedisUtil.getJedis();
        //pool.auth("admin");
        pool.set("name", "gallen");
        System.out.println(pool.get("name"));
    }
}
