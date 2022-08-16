package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeCourseOrderWindowBean;
import com.atguigu.edu.realtime.util.*;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DwsTradeCourseOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_order_detail 主题读取数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_course_order_window";
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

        // TODO 6. 转换数据结构
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> mappedBeanStream = removedReplicatedStream.map(
                jsonObj -> DwsTradeCourseOrderWindowBean.builder()
                        .courseId(jsonObj.getString("course_id"))
                        .orderTotalAmount(new BigDecimal(jsonObj.getString("final_amount")))
                        .ts(jsonObj.getLong("ts") * 1000L)
                        .build()
        );

        // TODO 7. 设置水位线
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withWatermarkStream = mappedBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCourseOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCourseOrderWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeCourseOrderWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })

        );

        // TODO 8. 按照 courseId 分组
        KeyedStream<DwsTradeCourseOrderWindowBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsTradeCourseOrderWindowBean::getCourseId);

        // TODO 9. 开窗
        WindowedStream<DwsTradeCourseOrderWindowBean, String, TimeWindow> windowStream
                = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public DwsTradeCourseOrderWindowBean reduce(DwsTradeCourseOrderWindowBean value1, DwsTradeCourseOrderWindowBean value2) throws Exception {
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTradeCourseOrderWindowBean, DwsTradeCourseOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String jsonStr, Context context, Iterable<DwsTradeCourseOrderWindowBean> elements, Collector<DwsTradeCourseOrderWindowBean> out) throws Exception {
                        for (DwsTradeCourseOrderWindowBean element : elements) {
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

        // TODO 11. 补充维度
        // 11.1 补充课程名称及科目 ID
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withSubjectInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_course_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeCourseOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCourseName(dimJsonObj.getString("course_name".toUpperCase()));
                        bean.setSubjectId(dimJsonObj.getString("subject_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeCourseOrderWindowBean bean) {
                        return bean.getCourseId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // 11.2 补充科目名称及类别 ID
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withCategoryInfoStream = AsyncDataStream.unorderedWait(
                withSubjectInfoStream,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_base_subject_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeCourseOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setSubjectName(dimJsonObj.getString("subject_name".toUpperCase()));
                        bean.setCategoryId(dimJsonObj.getString("category_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeCourseOrderWindowBean bean) {
                        return bean.getSubjectId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // 11.3 补充类别名称
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> finalStream = AsyncDataStream.unorderedWait(
                withCategoryInfoStream,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>("dim_base_category_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeCourseOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCategoryName(dimJsonObj.getString("category_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeCourseOrderWindowBean bean) {
                        return bean.getCategoryId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 12. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_course_order_window values(?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
