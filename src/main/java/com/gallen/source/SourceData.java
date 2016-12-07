package com.gallen.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import redis.clients.jedis.Jedis;

/**
 * Created by gallenvara on 16/11/26.
 */
public class SourceData {
    //Redis服务器IP
    private static String REDIS_HOST = "127.0.0.1";

    //Redis的端口号
    private static int REDIS_PORT = 6379;

    //访问密码
    private static String AUTH = "admin";
    private static FlinkJedisPoolConfig jedisPoolConfig;
    static StreamExecutionEnvironment sEnv;
    private static Jedis jedis;

    static {
        try {
            jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                    .setHost(REDIS_HOST)
                    .setPort(REDIS_PORT).build();
            jedis = new Jedis(REDIS_HOST, REDIS_PORT);
            sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            sEnv.setParallelism(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //BasicConfigurator.configure();

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple2<Integer, Integer>> source = env.fromElements(new Tuple2<>(1,1), new Tuple2<>(2,2));
        DataSet<Tuple2<Integer, Integer>> result = source.filter((FilterFunction<Tuple2<Integer, Integer>>) value -> value.f0 % 2 == 0);
        DataSet<Tuple4<Integer, Integer, Integer, String>> dataSet = env
                .readCsvFile("/Users/gallenvara/Downloads/QCLCD201611/201611remarks.txt").ignoreFirstLine()
                .types(Integer.class, Integer.class, Integer.class, String.class);
        //dataSet.print();
        DataStream<String> streamSource = sEnv.readTextFile("/Users/gallenvara/Downloads/QCLCD201611/201611remarks.txt");

        DataStream<Tuple4<Integer, Integer, Integer, String>> dataStream1 = streamSource
                .filter((FilterFunction<String>) value -> value.charAt(0) != 'W')
                .flatMap(new FlatMapFunction<String, Tuple4<Integer, Integer, Integer, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple4<Integer, Integer, Integer, String>> collector) throws Exception {
                        String[] strings = s.split(",");
                        String str = "";
                        if (strings.length == 4) {
                            str = strings[3];
                        }
                        collector.collect(new Tuple4<>(
                                1,
                                Integer.parseInt(strings[1]),
                                Integer.parseInt(strings[2]),
                                str));
                    }
                })
                .filter((FilterFunction<Tuple4<Integer, Integer, Integer, String>>) value -> value.f2 % 2313 == 0)
                .keyBy(1)
                .sum(0);

        DataStream<Long> dataStream2 = sEnv.generateSequence(1, 9);
        RedisSink<Tuple4<Integer, Integer, Integer, String>> redisSink = new RedisSink<>(jedisPoolConfig,
                new RedisCommandMapper(RedisCommand.SET));
        dataStream1.addSink(redisSink);
        sEnv.execute();
    }

    public static class RedisCommandMapper implements RedisMapper<Tuple4<Integer, Integer, Integer, String>> {
        private RedisCommand redisCommand;

        public RedisCommandMapper(RedisCommand redisCommand) {
            this.redisCommand = redisCommand;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(redisCommand);
        }

        @Override
        public String getKeyFromData(Tuple4<Integer, Integer, Integer, String> data) {
            return String.valueOf(data.f1);
        }

        @Override
        public String getValueFromData(Tuple4<Integer, Integer, Integer, String> data) {
            return String.valueOf(data.f0);
        }
    }
}
