package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeProvinceOrderWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_order_detail 主题读取数据
        String topic = "topic_db";
        String groupId = "dws_trade_province_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 筛选订单表数据并转换数据结构
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> mappedStream = source
                .process(new ProcessFunction<String, DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getString("table");
                        String type = jsonObj.getString("type");
                        if (table.equals("order_info") && type.equals("insert")) {
                            JSONObject data = jsonObj.getJSONObject("data");
                            out.collect(DwsTradeProvinceOrderWindowBean.builder()
                                    .provinceId(data.getString("province_id"))
                                    .orderTotalAmount(new BigDecimal(data.getString("final_amount")))
                                    .userId(data.getString("user_id"))
                                    .orderCount(1L)
                                    .ts(jsonObj.getLong("ts") * 1000L)
                                    .build());
                        }
                    }
                });

        // TODO 4. 按照 province_id 及 user_id 分组
        KeyedStream<DwsTradeProvinceOrderWindowBean, String> keyedStream = mappedStream.keyBy(
                bean -> bean.getProvinceId() + bean.getUserId()
        );

        // TODO 5. 统计各省份下单独立用户数
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean>() {

                    private ValueState<String> userOrderLastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> userOrderLastDtStateDescriptor
                                = new ValueStateDescriptor<>("user_order_last_dt_state", String.class);
                        userOrderLastDtStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build()
                        );
                        this.userOrderLastDtState = getRuntimeContext().getState(
                                userOrderLastDtStateDescriptor
                        );
                    }

                    @Override
                    public void processElement(DwsTradeProvinceOrderWindowBean bean, Context context, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        String curDate = DateFormatUtil.toDate(bean.getTs());
                        String userOrderLastDt = userOrderLastDtState.value();
                        Long orderUuCount = 0L;
                        if (userOrderLastDt == null || userOrderLastDt.compareTo(curDate) < 0) {
                            orderUuCount = 1L;
                            userOrderLastDtState.update(curDate);
                        }
                        bean.setOrderUuCount(orderUuCount);
                        out.collect(bean);
                    }
                }
        );

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeProvinceOrderWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeProvinceOrderWindowBean jsonObj, long recordTimestamp) {
                                return jsonObj.getTs();
                            }
                        })
        );

        // TODO 7. 分组
        KeyedStream<DwsTradeProvinceOrderWindowBean, String> provinceKeyedStream =
                withWatermarkStream.keyBy(DwsTradeProvinceOrderWindowBean::getProvinceId);

        // TODO 8. 开窗
        WindowedStream<DwsTradeProvinceOrderWindowBean, String, TimeWindow> windowStream =
                provinceKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> reduecedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public DwsTradeProvinceOrderWindowBean reduce(DwsTradeProvinceOrderWindowBean value1, DwsTradeProvinceOrderWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeProvinceOrderWindowBean> elements, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        for (DwsTradeProvinceOrderWindowBean element : elements) {
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

        // TODO 10. 补充省份名称
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reduecedStream,
                new DimAsyncFunction<DwsTradeProvinceOrderWindowBean>("dim_base_province".toUpperCase()) {
                    @Override
                    public void join(DwsTradeProvinceOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setProvinceName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeProvinceOrderWindowBean bean) {
                        return bean.getProvinceId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 11. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
