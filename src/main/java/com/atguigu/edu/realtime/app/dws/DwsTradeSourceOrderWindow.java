package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeSourceOrderWindowBean;
import com.atguigu.edu.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeSourceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_order_detail 主题读取数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_source_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 过滤 null 数据及字段不完整数据并转换数据结构
        SingleOutputStreamOperator<JSONObject> filteredStream = source.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String sourceId = jsonObj.getString("source_id");
                            if (sourceId != null) {
                                out.collect(jsonObj);
                            }
                        }
                    }
                }
        );

        // TODO 4. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = filteredStream.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 5. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<JSONObject> lastValueDescriptor =
                                new ValueStateDescriptor<JSONObject>("last_value", JSONObject.class);
                        lastValueDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                        lastValueState = getRuntimeContext().getState(lastValueDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 10L);
                            lastValueState.update(jsonObj);
                        } else {
                            String currentTs = jsonObj.getString("row_op_ts");
                            String lastTs = lastValue.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(currentTs, lastTs) >= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, context, out);
                        JSONObject jsonObj = lastValueState.value();
                        out.collect(jsonObj);
                    }
                }
        );

        // TODO 6. 按照 source_id + user_id 分组
        KeyedStream<JSONObject, String> sourceIdKeyedStream =
                processedStream.keyBy(r -> r.getString("source_id") + r.getString("user_id"));

        // TODO 7. 统计下单独立用户数
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> orderUuCountStream = sourceIdKeyedStream.process(
                new KeyedProcessFunction<String, JSONObject, DwsTradeSourceOrderWindowBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> lastOrderDtDescriptor
                                = new ValueStateDescriptor<>("last_order_dt", String.class);
                        lastOrderDtDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                        lastOrderDtState = getRuntimeContext().getState(lastOrderDtDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        long ts = jsonObj.getLong("ts") * 1000L;
                        String curDate = DateFormatUtil.toDate(ts);
                        Long orderUuCount = 0L;
                        if (lastOrderDt == null || lastOrderDt.compareTo(curDate) < 0) {
                            orderUuCount = 1L;
                            lastOrderDtState.update(curDate);
                        }
                        String sourceId = jsonObj.getString("source_id");
                        BigDecimal orderTotalAmount = new BigDecimal(jsonObj.getString("final_amount"));
                        String orderId = jsonObj.getString("order_id");
                        out.collect(DwsTradeSourceOrderWindowBean.builder()
                                .sourceId(sourceId)
                                .orderTotalAmount(orderTotalAmount)
                                .orderUuCount(orderUuCount)
                                .orderId(orderId)
                                .ts(ts)
                                .build());
                    }
                }
        );

        // TODO 8. 按照 orderId 分组
        KeyedStream<DwsTradeSourceOrderWindowBean, String> keyByOrderIdStream = orderUuCountStream.keyBy(DwsTradeSourceOrderWindowBean::getOrderId);

        // TODO 9. 统计订单数
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> withOrderCountStream = keyByOrderIdStream.process(
                new KeyedProcessFunction<String, DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean>() {

                    private ValueState<String> orderIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> orderIdStateDescriptor
                                = new ValueStateDescriptor<>("order_id_state", String.class);
                        orderIdStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.minutes(30L)).build()
                        );
                        orderIdState = getRuntimeContext().getState(orderIdStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTradeSourceOrderWindowBean bean, Context context, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                        String lastOrderId = orderIdState.value();
                        String orderId = bean.getOrderId();
                        Long orderCount = 0L;
                        if (lastOrderId == null) {
                            orderCount = 1L;
                            orderIdState.update(orderId);
                        }
                        bean.setOrderCount(orderCount);
                        out.collect(bean);
                    }
                }
        );

        // TODO 10. 设置水位线
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> withWatermarkStream = withOrderCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeSourceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeSourceOrderWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeSourceOrderWindowBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // TODO 11. 分组
        KeyedStream<DwsTradeSourceOrderWindowBean, String> sourceKeyedStream
                = withWatermarkStream.keyBy(DwsTradeSourceOrderWindowBean::getSourceId);

        // TODO 12. 开窗
        WindowedStream<DwsTradeSourceOrderWindowBean, String, TimeWindow> windowStream
                = sourceKeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 13. 聚合
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeSourceOrderWindowBean>() {
                    @Override
                    public DwsTradeSourceOrderWindowBean reduce(DwsTradeSourceOrderWindowBean value1, DwsTradeSourceOrderWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTradeSourceOrderWindowBean> elements, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                        for (DwsTradeSourceOrderWindowBean element : elements) {
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

        // TODO 14. 补充来源名称信息
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsTradeSourceOrderWindowBean>("dim_base_source".toUpperCase()) {
                    @Override
                    public void join(DwsTradeSourceOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setSourceName(dimJsonObj.getString("source_site".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeSourceOrderWindowBean bean) {
                        return bean.getSourceId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // TODO 15. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_source_order_window values(?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
