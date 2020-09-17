package com.lhy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSinkTest {


    public static void main(String[] args) throws Exception {

        String READ_TOPIC = "test";


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Customer> source = env.addSource(new StreamParallelSource());

        DataStream<JSONObject> stream = source.map(new MapFunction<Customer, JSONObject>() {
            @Override
            public JSONObject map(Customer customer) throws Exception {
                String jsonString = JSONObject.toJSONString(customer, SerializerFeature.WriteDateUseDateFormat);

                System.out.println("current dowith:"+jsonString);

                JSONObject json = JSON.parseObject(jsonString);
                return json;
            }
        });


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.1.20.44", 9200, "http"));

        ElasticsearchSink.Builder<JSONObject> esBuilder = new ElasticsearchSink.Builder(httpHosts,
                new ElasticsearchSinkFunction<JSONObject>() {

                    @Override
                    public void process(JSONObject s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                        requestIndexer.add(createIndex(s));
                    }

                    public IndexRequest createIndex(JSONObject element){

                        Map map = new HashMap<>();
                        map.put("data", element);

                        return Requests.indexRequest().index("test").type("student").source(map);
                    }
                });

        esBuilder.setBulkFlushMaxActions(1);

        esBuilder.setRestClientFactory(new RestClientFactoryImpl());


        stream.addSink(esBuilder.build());

        env.execute("run kafka sink to es");



    }


    public static final class RestClientFactoryImpl implements RestClientFactory{

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {

            Header[] header = new BasicHeader[]{new BasicHeader("Content-Type", "application/json")};
            restClientBuilder.setDefaultHeaders(header);
            restClientBuilder.setMaxRetryTimeoutMillis(9000);

        }
    }




}
