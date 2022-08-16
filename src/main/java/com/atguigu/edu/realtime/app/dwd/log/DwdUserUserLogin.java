package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwdUserUserLoginBean;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdUserUserLogin {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_traffic_page_log 主题读取数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_user_user_login";
        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3. 转换数据结构 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 4. 过滤未登录页面
        SingleOutputStreamOperator<JSONObject> filteredStream = mappedStream.filter(
                jsonObj -> jsonObj.getJSONObject("common").getString("uid") != null
        );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = filteredStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 6. 按照 session_id 分组
        KeyedStream<JSONObject, String> keyedStream =
                withWatermarkStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("sid"));

        // TODO 7. 运用 Flink 状态编程和定时器保留各会话的登陆日志
        SingleOutputStreamOperator<JSONObject> loginStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> firstLoginLog;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("first_login_log", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                        firstLoginLog = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject logInState = firstLoginLog.value();

                        Long ts = jsonObj.getLong("ts");
                        String thisTime = DateFormatUtil.toYmdHms(ts);
                        if (logInState == null) {
                            context.timerService().registerEventTimeTimer(ts + 10 * 1000L);
                            firstLoginLog.update(jsonObj);
                        } else {
                            String timeInState = DateFormatUtil.toYmdHms(logInState.getLong("ts"));
                            if (thisTime.compareTo(timeInState) < 0) {
                                firstLoginLog.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, context, out);
                        out.collect(firstLoginLog.value());
                    }
                }
        );

        // TODO 8. 转换数据结构
        SingleOutputStreamOperator<String> finalStream = loginStream.map(
                jsonObj -> {
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    String loginTime = DateFormatUtil.toYmdHms(ts);
                    String dateId = loginTime.substring(0, 10);

                    DwdUserUserLoginBean dwdUserUserLoginBean = DwdUserUserLoginBean.builder()
                            .userId(common.getString("uid"))
                            .dateId(dateId)
                            .loginTime(loginTime)
                            .channel(common.getString("ch"))
                            .provinceId(common.getString("ar"))
                            .versionCode(common.getString("vc"))
                            .midId(common.getString("mid"))
                            .brand(common.getString("ba"))
                            .model(common.getString("md"))
                            .sourceId(common.getString("sc"))
                            .operatingSystem(common.getString("os"))
                            .ts(ts)
                            .build();

                    return JSON.toJSONString(dwdUserUserLoginBean);
                }
        );

        // TODO 9. 将数据写入 Kafka dwd_user_user_login 主题
        String sinkTopic = "dwd_user_user_login";
        finalStream.addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();
    }
}
