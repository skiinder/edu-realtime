package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class
DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 kafka dwd_traffic_page_log 主题读取日志数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        // 测试数据
        /*DataStream<String> kafkaDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/

        // TODO 3. 转换结构
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLog.map(JSON::parseObject);

        // TODO 4. 设置水位线，用于用户跳出统计
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 5. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(jsonOjb -> jsonOjb.getJSONObject("common").getString("mid"));

        // TODO 6. 定义 CEP 匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }
                // 上文调用了同名 Time 类，此处需要使用全类名
        ).within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));

        // TODO 7. 把 Pattern 应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 8. 提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {
        };
        SingleOutputStreamOperator<JSONObject> flatSelectStream = patternStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                        JSONObject element = pattern.get("first").get(0);
                        out.collect(element);

                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                        JSONObject element = pattern.get("first").get(0);
                        out.collect(element);
                    }
                }
        );

        DataStream<JSONObject> timeOutDStream = flatSelectStream.getSideOutput(timeoutTag);

        // TODO 9. 合并两个流并将数据写出到 Kafka
        DataStream<JSONObject> unionDStream = flatSelectStream.union(timeOutDStream);
        String targetTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(targetTopic);
        SingleOutputStreamOperator<String> finalStream = unionDStream.map(JSONAware::toJSONString);
        finalStream.addSink(kafkaProducer);

        env.execute();
    }
}
