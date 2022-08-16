package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsUserRegisterWindowBean;
import com.atguigu.edu.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;

public class DwsUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_user_user_register 主题读取数据
        String topic = "dwd_user_user_register";
        String groupId = "dws_user_register_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 过滤 null 数据，按照 user_id 分组
        SingleOutputStreamOperator<String> withoutNullStream = source.filter(Objects::nonNull);
        KeyedStream<String, String> keyedByUserIdStream
                = withoutNullStream.keyBy(jsonStr -> JSON.parseObject(jsonStr).getString("user_id"));

        // TODO 4. 运用 Flink 状态编程对数据去重
        SingleOutputStreamOperator<JSONObject> removedReplicatedStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, String, JSONObject>() {

                    private ValueState<JSONObject> lastLogState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<JSONObject> lastLogStateDescriptor
                                = new ValueStateDescriptor<>("last_log_state", JSONObject.class);
                        lastLogStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.minutes(1L)).build()
                        );
                        lastLogState = getRuntimeContext().getState(lastLogStateDescriptor);
                    }

                    @Override
                    public void processElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSONObject.parseObject(jsonStr);

                        // 获取状态中的数据
                        JSONObject lastLog = lastLogState.value();

                        // 若状态为空则说明是相同 user_id 的第一条数据，此时注册一个定时器，用于向下游发送数据
                        if (lastLog == null) {
                            // 注册定时器
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                            // 更新状态
                            lastLogState.update(jsonObj);
                        } else {
                            // 获取状态中数据和当前数据的操作时间
                            String lastOpTs = lastLog.getString("row_op_ts");
                            String opTs = jsonObj.getString("row_op_ts");
                            // 若当前数据的时间大于等于状态中数据的时间，说明当前数据生成时间更晚，更新状态
                            if (TimestampLtz3CompareUtil.compare(opTs, lastOpTs) >= 0) {
                                lastLogState.update(jsonObj);
                            }

                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, context, out);
                        JSONObject lastLog = lastLogState.value();
                        out.collect(lastLog);
                    }

                }
        );

        // TODO 5. 转换数据结构
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> mappedStream = removedReplicatedStream.map(jsonObj -> {
            Long ts = jsonObj.getLong("ts") * 1000L;
            return DwsUserRegisterWindowBean.builder()
                    .registerCount(1L)
                    .ts(ts)
                    .build();
        });

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsUserRegisterWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsUserRegisterWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsUserRegisterWindowBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        // TODO 7. 开窗
        AllWindowedStream<DwsUserRegisterWindowBean, TimeWindow> windowStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsUserRegisterWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsUserRegisterWindowBean>() {
                    @Override
                    public DwsUserRegisterWindowBean reduce(DwsUserRegisterWindowBean value1, DwsUserRegisterWindowBean value2) throws Exception {
                        value1.setRegisterCount(value1.getRegisterCount() + value2.getRegisterCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsUserRegisterWindowBean, DwsUserRegisterWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsUserRegisterWindowBean> elements, Collector<DwsUserRegisterWindowBean> out) throws Exception {
                        for (DwsUserRegisterWindowBean element : elements) {
                            String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                            String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 9. 写出到 ClickHouse
        reducedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_user_register_window values(?,?,?,?)")
        );

        env.execute();
    }
}
