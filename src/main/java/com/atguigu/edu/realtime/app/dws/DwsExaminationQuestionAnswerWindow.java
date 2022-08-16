package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsExaminationQuestionAnswerWindowBean;
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

public class DwsExaminationQuestionAnswerWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_examination_test_question 主题读取数据
        String topic = "dwd_examination_test_question";
        String groupId = "dws_examination_question_answer_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转化数据结构
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String questionId = jsonObj.getString("question_id");
                    String isCorrect = jsonObj.getString("is_correct");
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    Long correctAnswerCount = isCorrect.equals("1") ? 1L : 0L;
                    return DwsExaminationQuestionAnswerWindowBean.builder()
                            .question_id(questionId)
                            .correctAnswerCount(correctAnswerCount)
                            .answer_count(1L)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationQuestionAnswerWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsExaminationQuestionAnswerWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsExaminationQuestionAnswerWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 5. 分组
        KeyedStream<DwsExaminationQuestionAnswerWindowBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsExaminationQuestionAnswerWindowBean::getQuestion_id);

        // TODO 6. 开窗
        WindowedStream<DwsExaminationQuestionAnswerWindowBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsExaminationQuestionAnswerWindowBean>() {
                    @Override
                    public DwsExaminationQuestionAnswerWindowBean reduce(DwsExaminationQuestionAnswerWindowBean value1, DwsExaminationQuestionAnswerWindowBean value2) throws Exception {
                        value1.setAnswer_count(value1.getAnswer_count() + value2.getAnswer_count());
                        value1.setCorrectAnswerCount(value1.getCorrectAnswerCount() + value2.getCorrectAnswerCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsExaminationQuestionAnswerWindowBean> elements, Collector<DwsExaminationQuestionAnswerWindowBean> out) throws Exception {
                        for (DwsExaminationQuestionAnswerWindowBean element : elements) {
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

        // TODO 8. 补充题目内容
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsExaminationQuestionAnswerWindowBean>("dim_test_question_info".toUpperCase()) {
                    @Override
                    public void join(DwsExaminationQuestionAnswerWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setQuestion_txt(dimJsonObj.getString("question_txt".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsExaminationQuestionAnswerWindowBean bean) {
                        return bean.getQuestion_id();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 9. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_examination_question_answer_window " +
                        "values(?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
