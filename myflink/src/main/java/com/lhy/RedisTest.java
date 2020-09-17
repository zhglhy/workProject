package com.lhy;

        import redis.clients.jedis.Jedis;

public class RedisTest {


    public static void main(String[] args) {

        Jedis jedis = new Jedis("127.0.0.1");

        System.out.println("redis is running:" + jedis.ping());

        System.out.println("result:"+jedis.hgetAll("flink"));

    }


}
