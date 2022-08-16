package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationPaperScoreDurationExamWindowBean;
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

public class DwsExaminationPaperScoreDurationExamWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 状态后端设置及环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_examination_test_paper 主题读取数据
        String topic = "dwd_examination_test_paper";
        String groupId = "dws_examination_paper_score_duration_exam_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String paperId = jsonObj.getString("paper_id");
                    Long score = (long)jsonObj.getDouble("score").doubleValue();
                    Long ts = jsonObj.getLong("ts") * 1000L;

                    String scoreDuration;

                    if (score < 60) {
                        scoreDuration = "[0, 60)";
                    } else if (score < 80) {
                        scoreDuration = "[60, 80)";
                    } else if (score < 90) {
                        scoreDuration = "[80, 90)";
                    } else if (score < 95) {
                        scoreDuration = "[90, 95)";
                    } else {
                        scoreDuration = "[95, 100]";
                    }

                    return DwsExaminationPaperScoreDurationExamWindowBean.builder()
                            .paper_id(paperId)
                            .score_duration(scoreDuration)
                            .user_count(1L)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperScoreDurationExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationPaperScoreDurationExamWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationPaperScoreDurationExamWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 5. 分组
        KeyedStream<DwsExaminationPaperScoreDurationExamWindowBean, String> keyedStream = withWatermarkStream.keyBy(
                bean -> bean.getPaper_id() + bean.getScore_duration()
        );

        // TODO 6. 开窗
        WindowedStream<DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean reduce(DwsExaminationPaperScoreDurationExamWindowBean value1, DwsExaminationPaperScoreDurationExamWindowBean value2) throws Exception {
                        value1.setUser_count(value1.getUser_count() + value2.getUser_count());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsExaminationPaperScoreDurationExamWindowBean> elements, Collector<DwsExaminationPaperScoreDurationExamWindowBean> out) throws Exception {
                        for (DwsExaminationPaperScoreDurationExamWindowBean element : elements) {
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

        // TODO 8. 补充试卷名称
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsExaminationPaperScoreDurationExamWindowBean>("dim_test_paper".toUpperCase()) {
                    @Override
                    public void join(DwsExaminationPaperScoreDurationExamWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setPaper_title(dimJsonObj.getString("paper_title".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsExaminationPaperScoreDurationExamWindowBean bean) {
                        return bean.getPaper_id();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 9. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_examination_paper_score_duration_exam_window" +
                        "values(?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
