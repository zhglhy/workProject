package com.lhy;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.table.api.TableEnvironment;

public class CsvTest {


    public static void main(String[] args) {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        CsvReader input = env.readCsvFile("");



//        ((BatchTableEnvironment) tableEnv).registerDataSet("student", input);


    }

}
