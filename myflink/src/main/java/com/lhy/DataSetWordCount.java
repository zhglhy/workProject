package com.lhy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DataSetWordCount {


    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements("aaa bbb ccc",
                "ddd eeee", "fff hh", "aaa ggg");


        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);


        counts.printToErr();

    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            String[] tokens = value.toLowerCase().split("\\W+");
            for(String token : tokens){

                if(token.length()>0){
                    out.collect(new Tuple2<>(token, 1));
                }
            }

        }
    }


}
