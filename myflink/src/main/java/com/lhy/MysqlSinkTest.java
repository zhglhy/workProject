package com.lhy;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class MysqlSinkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.20.39:9092");
        properties.setProperty("group.id", "test-consumer-group");

//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(), properties));

        DataStream<Tuple3<Integer, String, Integer>> sourceStream = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNotBlank(s);
            }
        }).map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(String s) throws Exception {
                String[] args = s.split(",");

                return new Tuple3<Integer, String, Integer>(Integer.valueOf(args[0]), args[1], Integer.valueOf(args[2]));
            }
        });

        sourceStream.addSink(new MysqlSink());
        env.execute("data sink to mysql");




    }

    public static final class MysqlSink extends RichSinkFunction<Tuple3<Integer, String, Integer>>{

        private Connection connection;
        private PreparedStatement preparedStatement;
        String userName = "u_risk_engine";
        String password = "gmj<0b59;rX$ZV45^TB]";
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.1.20.240:3306/saic_risk_engine?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";


        @Override
        public void invoke(Tuple3<Integer, String, Integer> value, Context context) throws Exception {
            Class.forName(driverName);
            connection = DriverManager.getConnection(url, userName, password);
            String sql = "insert into test(card,name,phone) values(?,?,?)";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, value.f0);
            preparedStatement.setString(2, value.f1);
            preparedStatement.setInt(3, value.f2);
            preparedStatement.executeUpdate();
            if(preparedStatement != null){
                preparedStatement.close();
            }
            if(connection != null){
                connection.close();
            }

        }
    }

}
