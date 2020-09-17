package com.lhy;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AggTest {

    private static final String[] SUBJECT = { "语文", "数学", "英语", "物理", "化学" };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(5);

        DataStream<Tuple3<String, String, Integer>> source = env.addSource(new SourceFunction<Tuple3<String, String, Integer>>() {

            private volatile boolean isRunning = true;
            private final Random random = new Random();

            @Override
                public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {

                    while(isRunning){
                        TimeUnit.SECONDS.sleep(1);
                        ctx.collect(Tuple3.of("xiaobai", SUBJECT[random.nextInt(SUBJECT.length)], (int)(Math.random()*50)+50));
                    }
                }

                @Override
                public void cancel() {

                    isRunning = false;
            }
        });

        DataStream<Tuple4<String, String, Integer, Double>> score = source.keyBy(1)
                .countWindow(5)
                .aggregate(new AverageAggrate());

        score.print();

        env.execute();


    }

    public static class AverageAggrate implements AggregateFunction<Tuple3<String, String, Integer>, Tuple4<String, String, Integer, Integer>, Tuple4<String, String, Integer, Double>> {


        @Override
        public Tuple4<String, String, Integer, Integer> createAccumulator() {
            return new Tuple4<>("", "", 0, 0);
        }

        @Override
        public Tuple4<String, String, Integer, Integer> add(Tuple3<String, String, Integer> value, Tuple4<String, String, Integer, Integer> acc) {
            return new Tuple4<>(value.f0, value.f1, acc.f2+value.f2, acc.f3+1);
        }

        @Override
        public Tuple4<String, String, Integer, Double> getResult(Tuple4<String, String, Integer, Integer> acc) {
            return Tuple4.of(acc.f0, acc.f1, acc.f2, (double)acc.f2/acc.f3);
        }

        @Override
        public Tuple4<String, String, Integer, Integer> merge(Tuple4<String, String, Integer, Integer> stringStringIntegerIntegerTuple4, Tuple4<String, String, Integer, Integer> acc1) {
            return null;
        }
    }

}
