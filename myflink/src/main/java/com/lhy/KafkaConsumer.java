package com.lhy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.20.39:9092");
        properties.setProperty("group.id", "test-consumer-group");

        properties.setProperty("zookeeper.connect", "10.1.20.39:2181");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


//        HashMap<KafkaTopicPartition, Long> kafkaTopicPartitionMap = new HashMap<>();
//        kafkaTopicPartitionMap.put(new KafkaTopicPartition("test",0),10L);
//        kafkaTopicPartitionMap.put(new KafkaTopicPartition("test",1),0L);
//        kafkaTopicPartitionMap.put(new KafkaTopicPartition("test",2),0L);



        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(), properties);


//        consumer.setStartFromSpecificOffsets(kafkaTopicPartitionMap);

        DataStreamSource<String> text = env.addSource(consumer);


        text.map(new MapFunction<String, Object>() {

            @Override
            public Object map(String s) throws Exception {
//                System.out.println("consumer msg:" + s);
                Thread.sleep(2000);
                return s;
            }
        });


        text.print().setParallelism(1);

        env.execute("start kafka consumer....");





    }
}
