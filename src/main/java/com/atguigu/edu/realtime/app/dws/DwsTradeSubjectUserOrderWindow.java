package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTradeSubjectUserOrderWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * description:
 * Created by 铁盾 on 2022/6/23
 */
public class DwsTradeSubjectUserOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_order_detail 主题读取数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_subject_user_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    return DwsTradeSubjectUserOrderWindowBean.builder()
                            .userId(jsonObj.getString("user_id"))
                            .courseId(jsonObj.getString("course_id"))
                            .orderId(jsonObj.getString("order_id"))
                            .orderTotalAmount(new BigDecimal(jsonObj.getString("final_amount")))
                            .ts(jsonObj.getLong("ts") * 1000L)
                            .build();
                }
        );

        // TODO 4. 关联课程维度表，获取科目信息
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> withSubjectInfoStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTradeSubjectUserOrderWindowBean>("dim_course_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeSubjectUserOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        String subjectId = dimJsonObj.getString("subject_id".toUpperCase());
                        bean.setSubjectId(subjectId);
                    }

                    @Override
                    public String getKey(DwsTradeSubjectUserOrderWindowBean bean) {
                        return bean.getCourseId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
                );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> withWatermarkStream = withSubjectInfoStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeSubjectUserOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeSubjectUserOrderWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeSubjectUserOrderWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })

        );

        // TODO 6. 按照 subjectId 和 orderId 分组
        KeyedStream<DwsTradeSubjectUserOrderWindowBean, String> keyedStream = withWatermarkStream.keyBy(
                bean -> bean.getSubjectId() + bean.getOrderId()
        );

        // TODO 7. 统计各科目的订单数
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTradeSubjectUserOrderWindowBean, DwsTradeSubjectUserOrderWindowBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(DwsTradeSubjectUserOrderWindowBean bean, Context ctx, Collector<DwsTradeSubjectUserOrderWindowBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        String curDate = DateFormatUtil.toDate(bean.getTs());
                        if (lastOrderDt == null) {
                            lastOrderDtState.update(curDate);
                            bean.setOrderCount(1L);
                        } else {
                            bean.setOrderCount(0L);
                        }
                        out.collect(bean);
                    }
                }
        );

        // TODO 8. 按照 subject_id 分组
        KeyedStream<DwsTradeSubjectUserOrderWindowBean, String> subjectKeyedStream = processedStream.keyBy(bean -> bean.getSubjectId());

        // TODO 9. 开窗
        WindowedStream<DwsTradeSubjectUserOrderWindowBean, String, TimeWindow> windowStream = subjectKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeSubjectUserOrderWindowBean>() {
                    @Override
                    public DwsTradeSubjectUserOrderWindowBean reduce(DwsTradeSubjectUserOrderWindowBean value1, DwsTradeSubjectUserOrderWindowBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderTotalAmount(value1.getOrderTotalAmount().add(value2.getOrderTotalAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTradeSubjectUserOrderWindowBean, DwsTradeSubjectUserOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String jsonStr, Context context, Iterable<DwsTradeSubjectUserOrderWindowBean> elements, Collector<DwsTradeSubjectUserOrderWindowBean> out) throws Exception {
                        for (DwsTradeSubjectUserOrderWindowBean element : elements) {
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
        // 11.1 补充科目名称和类别 ID
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> withCateIdStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsTradeSubjectUserOrderWindowBean>("dim_base_subcjet_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeSubjectUserOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setSubjectName(dimJsonObj.getString("subject_name".toUpperCase()));
                        bean.setCategoryId(dimJsonObj.getString("category_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeSubjectUserOrderWindowBean bean) {
                        return bean.getSubjectId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 11.2 补充类别名称
        SingleOutputStreamOperator<DwsTradeSubjectUserOrderWindowBean> finalStream = AsyncDataStream.unorderedWait(
                withCateIdStream,
                new DimAsyncFunction<DwsTradeSubjectUserOrderWindowBean>("dim_base_category_info".toUpperCase()) {
                    @Override
                    public void join(DwsTradeSubjectUserOrderWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCategoryName(dimJsonObj.getString("category_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsTradeSubjectUserOrderWindowBean bean) {
                        return bean.getCategoryId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 12. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_subject_user_order_window values(?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
