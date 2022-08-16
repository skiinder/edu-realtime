package com.atguigu.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.MyBroadcastFunction;
import com.atguigu.edu.realtime.app.func.MyPhoenixSink;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * description:
 * Created by 铁盾 on 2022/6/13
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka topic_db 主题读取数据，封装为流，将其作为主流
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 清洗主流数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty_data", TypeInformation.of(String.class)) {
        };
        SingleOutputStreamOperator<JSONObject> cleandStream = source.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr
                            , Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.equals("bootstrap-start")
                                    && !type.equals("bootstrap-complete")) {
                                collector.collect(jsonObj);
                            }
                        } catch (Exception exception) {
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        DataStream<String> dirtyStream = cleandStream.getSideOutput(dirtyTag);
        String dirtyTopic = "dirty_topic";
        dirtyStream.addSink(KafkaUtil.getKafkaProducer(dirtyTopic));

        // TODO 4. 用 FlinkCDC 读取配置表数据，封装为流
        // 4.1 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("edu_config") // set captured database
                .tableList("edu_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 4.2 封装为流
        DataStreamSource<String> configStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlSource");

        // TODO 5. 广播配置流
        MapStateDescriptor<String, DimTableProcess> tableProcessDescriptor =
                new MapStateDescriptor<String, DimTableProcess>("config_state", String.class, DimTableProcess.class);
        BroadcastStream<String> broadcastStream = configStream.broadcast(tableProcessDescriptor);

        // TODO 6. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = cleandStream.connect(broadcastStream);

        // TODO 7. 处理维度表数据
        SingleOutputStreamOperator<JSONObject> processedStream = connectedStream.process(new MyBroadcastFunction(tableProcessDescriptor));

        // TODO 8. 将数据写入 Phoenix 表
        processedStream.addSink(new MyPhoenixSink());

        env.execute();
    }
}
