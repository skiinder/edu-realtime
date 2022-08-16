package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationPaperExamWindowBean;
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

public class DwsExaminationPaperExamWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 读取 Kafka dwd_examination_test_paper 主题数据
        String topic = "dwd_examination_test_paper";
        String groupId = "dws_examination_paper_exam_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String paperId = jsonObj.getString("paper_id");
                    Long score = (long) (jsonObj.getDouble("score")).doubleValue();
                    Long durationSec = jsonObj.getLong("duration_sec");
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    return DwsExaminationPaperExamWindowBean.builder()
                            .paperId(paperId)
                            .examTakenCount(1L)
                            .examTotalScore(score)
                            .examTotalDuringSec(durationSec)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationPaperExamWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationPaperExamWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 5. 分组
        KeyedStream<DwsExaminationPaperExamWindowBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsExaminationPaperExamWindowBean::getPaperId);

        // TODO 6. 开窗
        WindowedStream<DwsExaminationPaperExamWindowBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperExamWindowBean reduce(DwsExaminationPaperExamWindowBean value1, DwsExaminationPaperExamWindowBean value2) throws Exception {
                        value1.setExamTakenCount(value1.getExamTakenCount() + value2.getExamTakenCount());
                        value1.setExamTotalScore(value1.getExamTotalScore() + value2.getExamTotalScore());
                        value1.setExamTotalDuringSec(value1.getExamTotalDuringSec() + value2.getExamTotalDuringSec());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsExaminationPaperExamWindowBean> elements, Collector<DwsExaminationPaperExamWindowBean> out) throws Exception {
                        for (DwsExaminationPaperExamWindowBean element : elements) {
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

        // TODO 8. 补充维度信息
        // 8.1 补充试卷名称和课程 ID
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withCourseIdStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsExaminationPaperExamWindowBean>("dim_test_paper".toUpperCase()) {
                    @Override
                    public void join(DwsExaminationPaperExamWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setPaperTitle(dimJsonObj.getString("paper_title".toUpperCase()));
                        bean.setCourseId(dimJsonObj.getString("course_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsExaminationPaperExamWindowBean bean) {
                        return bean.getPaperId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // 8.2 补充课程名称
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> finalStream = AsyncDataStream.unorderedWait(
                withCourseIdStream,
                new DimAsyncFunction<DwsExaminationPaperExamWindowBean>("dim_course_info".toUpperCase()) {
                    @Override
                    public void join(DwsExaminationPaperExamWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCourseName(dimJsonObj.getString("course_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsExaminationPaperExamWindowBean bean) {
                        return bean.getCourseId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 9. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_examination_paper_exam_window values(?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
