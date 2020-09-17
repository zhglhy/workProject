package com.lhy;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaProducer {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.enableCheckpointing(5000);

        DataStreamSource<String> text = env.addSource(new MySource()).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.20.39:9092");

        FlinkKafkaProducer010<String> producer010 = new FlinkKafkaProducer010<String>(
                "10.1.20.39:9092","test", new SimpleStringSchema()
        );

        producer010.setWriteTimestampToKafka(true);

        text.addSink(producer010);

        env.execute();




    }

    public static final class MySource implements SourceFunction<String>{

        private Boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while(isRunning){
                List<String> books = new ArrayList<>();
                books.add("java");
                books.add("pathon");
                books.add("java3222");
                books.add("go+");
                books.add("java 11");
                int i = new Random().nextInt(5);
                ctx.collect(books.get(i));
                Thread.sleep(2000);
            }

        }

        @Override
        public void cancel() {

            isRunning = false;
        }
    }

}
