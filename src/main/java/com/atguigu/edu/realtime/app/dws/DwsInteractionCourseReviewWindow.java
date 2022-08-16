package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsInteractionCourseReviewWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsInteractionCourseReviewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 状态后端设置及环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_interaction_review 主题读取数据
        String topic = "dwd_interaction_review";
        String groupId = "dws_interaction_course_review_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构，统计度量值
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String courseId = jsonObj.getString("course_id");
                    Long reviewStars = jsonObj.getLong("review_stars");
                    long ts = jsonObj.getLong("ts") * 1000L;
                    Long goodReviewUserCount = reviewStars == 5L ? 1L : 0L;
                    Long reviewUserCount = 1L;
                    return DwsInteractionCourseReviewWindowBean.builder()
                            .courseId(courseId)
                            .reviewTotalStars(reviewStars)
                            .reviewUserCount(reviewUserCount)
                            .goodReviewUserCount(goodReviewUserCount)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsInteractionCourseReviewWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsInteractionCourseReviewWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsInteractionCourseReviewWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 5. 按照课程 ID 分组
        KeyedStream<DwsInteractionCourseReviewWindowBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsInteractionCourseReviewWindowBean::getCourseId);

        // TODO 6. 开窗
        WindowedStream<DwsInteractionCourseReviewWindowBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public DwsInteractionCourseReviewWindowBean reduce(DwsInteractionCourseReviewWindowBean value1, DwsInteractionCourseReviewWindowBean value2) throws Exception {
                        value1.setReviewTotalStars(value1.getReviewTotalStars() + value2.getReviewTotalStars());
                        value1.setReviewUserCount(value1.getReviewUserCount() + value2.getReviewUserCount());
                        value1.setGoodReviewUserCount(value1.getGoodReviewUserCount() + value2.getGoodReviewUserCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsInteractionCourseReviewWindowBean> elements, Collector<DwsInteractionCourseReviewWindowBean> out) throws Exception {
                        for (DwsInteractionCourseReviewWindowBean element : elements) {
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

        // TODO 8. 补充课程信息
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsInteractionCourseReviewWindowBean>("dim_course_info".toUpperCase()) {
                    @Override
                    public void join(DwsInteractionCourseReviewWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCourseName(dimJsonObj.getString("course_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsInteractionCourseReviewWindowBean bean) {
                        return bean.getCourseId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 9. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_interaction_course_review_window values(?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
