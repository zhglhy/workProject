package com.lhy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> frint = env.fromElements(new Person("zhansan", 34),
                new Person("lisi", 24),
                new Person("wangwu", 16));

        DataStream<Person> audit = frint.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age>18;
            }
        });

        audit.print();

        env.execute();


    }

    static class Person{

        private String name;
        private int age;

        public Person(){};

        public Person(String name, int age){
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

}
