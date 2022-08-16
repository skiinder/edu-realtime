package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradeOrderWindowBean;
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

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_order_detail 主题读取数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 过滤 null 数据并转换数据结构
        SingleOutputStreamOperator<String> filteredStream = source.filter(Objects::nonNull);
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredStream.map(JSON::parseObject);

        // TODO 4. 按照订单明细 ID 分组
        KeyedStream<JSONObject, String> keyedByOdIdStream = mappedStream.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 5. 去重
        SingleOutputStreamOperator<JSONObject> removedReplicatedStream = keyedByOdIdStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<JSONObject> lastDataStateDescriptor
                                = new ValueStateDescriptor<>("last_data_state", JSONObject.class);
                        lastDataStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.minutes(1L)).build()
                        );
                        lastDataState = getRuntimeContext().getState(lastDataStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject lastData = lastDataState.value();
                        if (lastData == null) {
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                            lastDataState.update(jsonObj);
                        } else {
                            String lastOpTs = lastData.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastOpTs, rowOpTs) <= 0) {
                                lastDataState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) throws IOException {
                        JSONObject lastData = lastDataState.value();
                        out.collect(lastData);
                    }
                }
        );

        // TODO 6. 按照 user_id 分组
        KeyedStream<JSONObject, String> keyedStream = removedReplicatedStream.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // TODO 7. 统计下单独立用户数和新用户数
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, DwsTradeOrderWindowBean>() {

                    private ValueState<String> orderLastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderLastDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("order_last_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<DwsTradeOrderWindowBean> out) throws Exception {
                        long ts = jsonObj.getLong("ts") * 1000;
                        String curDate = DateFormatUtil.toDate(ts);
                        String orderLastDt = orderLastDtState.value();
                        Long orderUvCount = 0L;
                        Long newOrderUserCount = 0L;
                        if (orderLastDt == null) {
                            orderUvCount = 1L;
                            newOrderUserCount = 1L;
                            orderLastDtState.update(curDate);
                        } else {
                            if (orderLastDt.compareTo(curDate) < 0) {
                                orderUvCount = 1L;
                                orderLastDtState.update(curDate);
                            }
                        }
                        if (orderUvCount != 0) {
                            out.collect(DwsTradeOrderWindowBean.builder()
                                    .orderUvCount(orderUvCount)
                                    .newOrderUserCount(newOrderUserCount)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
        );

        // TODO 8. 设置水位线
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeOrderWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeOrderWindowBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })
        );

        // TODO 9. 开窗
        AllWindowedStream<DwsTradeOrderWindowBean, TimeWindow> windowStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> reduecedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeOrderWindowBean>() {
                    @Override
                    public DwsTradeOrderWindowBean reduce(DwsTradeOrderWindowBean value1, DwsTradeOrderWindowBean value2) throws Exception {
                        value1.setOrderUvCount(value1.getOrderUvCount() + value2.getOrderUvCount());
                        value1.setNewOrderUserCount(value1.getNewOrderUserCount() + value2.getNewOrderUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsTradeOrderWindowBean, DwsTradeOrderWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradeOrderWindowBean> elements, Collector<DwsTradeOrderWindowBean> out) throws Exception {
                        for (DwsTradeOrderWindowBean element : elements) {
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

        // TODO 11. 写出到 ClickHouse
        reduecedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_order_window values(?,?,?,?,?)")
        );

        env.execute();
    }
}
