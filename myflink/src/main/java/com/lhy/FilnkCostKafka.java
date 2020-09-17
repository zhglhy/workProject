package com.lhy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FilnkCostKafka {


    public static void main(String[] args) throws Exception {



        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "10.1.20.39:9092");
//        properties.setProperty("zookeeper.connect", "10.1.20.39:2181");
        properties.setProperty("group.id", "test-consumer-group");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test",
                new SimpleStringSchema(), properties);


        DataStream<String> stream = env.addSource(myConsumer);

        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();


        counts.print();

        counts.addSink(new RedisSink<>(conf, new RedisSinkMapper()));


        env.execute("run kafka sink to redis");



    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            String[] words =  s.split("\\W+");
            for(String word : words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

    public final static class RedisSinkMapper implements RedisMapper<Tuple2<String, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

}
