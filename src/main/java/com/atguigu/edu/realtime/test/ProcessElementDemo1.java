package com.atguigu.edu.realtime.test;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by 铁盾 on 2021/12/13
 */
public class ProcessElementDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements(
                "{\"key\": \"1\", \"value\": \"white\"}",
                "{\"key\": \"2\", \"value\": \"yellow\"}",
                "{\"value\": \"blue\"}"
        );

        source
                .map(JSON::parseObject)
                .keyBy(r -> r.getString("key"))
//                .process(
//                        new KeyedProcessFunction<String, String, String>() {
//                            @Override
//                            public void open(Configuration parameters) throws Exception {
//                                super.open(parameters);
//                            }
//
//                            @Override
//                            public void close() throws Exception {
//                                super.close();
//                            }
//
//                            @Override
//                            public void processElement(String in, Context ctx, Collector<String> out) {
//                                if("white".equals(in)) {
//                                    out.collect(in);
//                                } else if ("black".equals(in)) {
//                                    out.collect(in);
//                                    out.collect(in);
//                                }
//                            }
//                        }
//                )
                .print("KeyedProcessFunction as flatMap");


        env.execute();
    }
}
