package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 kafka dwd_traffic_page_log 主题读取日志数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        // TODO 3. 转换结构
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLog.map(JSON::parseObject);

        // TODO 4. 过滤 last_page_id 不为 null 的数据
        SingleOutputStreamOperator<JSONObject> firstPageStream = mappedStream.filter(
                jsonObj -> jsonObj
                        .getJSONObject("page")
                        .getString("last_page_id") == null
        );

        // TODO 5. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = firstPageStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 6. 通过 Flink 状态编程过滤独立访客记录
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    private ValueState<String> lastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("last_visit_dt", String.class);
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig
                                        .newBuilder(Time.days(1L))
                                        // 设置在创建和更新状态时更新存活时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        lastVisitDt = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String visitDt = DateFormatUtil.toDate(jsonObj.getLong("ts"));
                        String lastDt = lastVisitDt.value();
                        if (lastDt == null || !lastDt.equals(visitDt)) {
                            lastVisitDt.update(visitDt);
                            return true;
                        }
                        return false;
                    }
                }
        );

        // TODO 7. 将独立访客数据写入
        // Kafka dwd_traffic_unique_visitor_detail 主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);
        SingleOutputStreamOperator<String> finalStream = filteredStream.map(JSONAware::toJSONString);
        finalStream.addSink(kafkaProducer);

        // TODO 8. 启动任务
        env.execute();
    }
}
