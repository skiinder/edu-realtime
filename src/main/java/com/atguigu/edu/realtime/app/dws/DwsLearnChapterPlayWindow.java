package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsLearnChapterPlayWindowBean;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsLearnChapterPlayWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_learn_play 主题读取数据
        String topic = "dwd_learn_play";
        String groupId = "dws_learn_chapter_play_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String videoId = jsonObj.getString("videoId");
                    String userId = jsonObj.getString("userId");
                    Long playSec = jsonObj.getLong("playSec");
                    Long ts = jsonObj.getLong("ts");
                    return DwsLearnChapterPlayWindowBean.builder()
                            .videoId(videoId)
                            .userId(userId)
                            .playCount(1L)
                            .playTotalSec(playSec)
                            .ts(ts)
                            .build();
                }
        );

        // TODO 4. 按照 user_id 分组
        KeyedStream<DwsLearnChapterPlayWindowBean, String> keyedByUserIdStream
                = mappedStream.keyBy(DwsLearnChapterPlayWindowBean::getUserId);

        // TODO 5. 统计独立用户数
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> withPlayUuCountStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, DwsLearnChapterPlayWindowBean, DwsLearnChapterPlayWindowBean>() {

                    private ValueState<String> lastPlayDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> lastPlayDtStateDescriptor
                                = new ValueStateDescriptor<>("last_play_dt_state", String.class);
                        lastPlayDtStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                                org.apache.flink.api.common.time.Time.days(1L)).build());
                        lastPlayDtState = getRuntimeContext().getState(lastPlayDtStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsLearnChapterPlayWindowBean bean, Context context, Collector<DwsLearnChapterPlayWindowBean> out) throws Exception {
                        String lastPlayDt = lastPlayDtState.value();
                        Long ts = bean.getTs();
                        String curDate = DateFormatUtil.toDate(ts);
                        if (lastPlayDt == null || lastPlayDt.compareTo(curDate) < 0) {
                            bean.setPlayUuCount(1L);
                            lastPlayDtState.update(curDate);
                        } else {
                            bean.setPlayUuCount(0L);
                        }
                        out.collect(bean);
                    }
                }
        );

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> withWatermarkStream = withPlayUuCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsLearnChapterPlayWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsLearnChapterPlayWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsLearnChapterPlayWindowBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 7. 分组
        KeyedStream<DwsLearnChapterPlayWindowBean, String> keyedStream
                = withWatermarkStream.keyBy(DwsLearnChapterPlayWindowBean::getVideoId);

        // TODO 8. 开窗
        WindowedStream<DwsLearnChapterPlayWindowBean, String, TimeWindow> windowStream
                = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsLearnChapterPlayWindowBean>() {
                    @Override
                    public DwsLearnChapterPlayWindowBean reduce(DwsLearnChapterPlayWindowBean value1, DwsLearnChapterPlayWindowBean value2) throws Exception {
                        value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
                        value1.setPlayTotalSec(value1.getPlayTotalSec() + value2.getPlayTotalSec());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsLearnChapterPlayWindowBean, DwsLearnChapterPlayWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsLearnChapterPlayWindowBean> elements, Collector<DwsLearnChapterPlayWindowBean> out) throws Exception {
                        for (DwsLearnChapterPlayWindowBean element : elements) {
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

        // TODO 10. 补充维度信息
        // 10.1 获取章节 ID
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> withChapterIdStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsLearnChapterPlayWindowBean>("dim_video_info".toUpperCase()) {
                    @Override
                    public void join(DwsLearnChapterPlayWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setChapterId(dimJsonObj.getString("chapter_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsLearnChapterPlayWindowBean bean) {
                        return bean.getVideoId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // 10.2 获取章节名称
        SingleOutputStreamOperator<DwsLearnChapterPlayWindowBean> finalStream = AsyncDataStream.unorderedWait(
                withChapterIdStream,
                new DimAsyncFunction<DwsLearnChapterPlayWindowBean>("dim_chapter_info".toUpperCase()) {
                    @Override
                    public void join(DwsLearnChapterPlayWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setChapterName(dimJsonObj.getString("chapter_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(DwsLearnChapterPlayWindowBean bean) {
                        return bean.getChapterId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 11. 写入 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_learn_chapter_play_window values(?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
