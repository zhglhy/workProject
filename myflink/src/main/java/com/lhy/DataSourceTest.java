package com.lhy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataSourceTest {


    public static void main(String[] args) {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> myInts = env.fromElements(1,2,3,4,5);








    }


}
