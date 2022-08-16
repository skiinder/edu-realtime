package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwdLearnPlayBean;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdLearnPlay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_traffic_play_pre_process 主题读取播放日志数据
        String topic = "dwd_traffic_play_pre_process";
        String groupId = "dwd_learn_play";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwdLearnPlayBean> mappedStream = source.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    JSONObject appVideo = jsonObj.getJSONObject("appVideo");
                    Long ts = jsonObj.getLong("ts");
                    return DwdLearnPlayBean.builder()
                            .sourceId(common.getString("sc"))
                            .provinceId(common.getString("ar"))
                            .userId(common.getString("uid"))
                            .operatingSystem(common.getString("os"))
                            .channel(common.getString("ch"))
                            .isNew(common.getString("is_new"))
                            .model(common.getString("md"))
                            .machineId(common.getString("mid"))
                            .versionCode(common.getString("vc"))
                            .brand(common.getString("ba"))
                            .sessionId(common.getString("sid"))
                            .playSec(appVideo.getInteger("play_sec"))
                            .positionSec(appVideo.getInteger("position_sec"))
                            .videoId(appVideo.getString("video_id"))
                            .ts(ts)
                            .build();

                }
        );

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwdLearnPlayBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwdLearnPlayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwdLearnPlayBean>() {
                                    @Override
                                    public long extractTimestamp(DwdLearnPlayBean dwdLearnPlayBean, long recordTimestamp) {
                                        return dwdLearnPlayBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 5. 按照 session_id 分组
        KeyedStream<DwdLearnPlayBean, String> keyedStream =
                withWatermarkStream.keyBy(DwdLearnPlayBean::getSessionId);

        // TODO 6. 开窗
        WindowedStream<DwdLearnPlayBean, String, TimeWindow> windowedStream =
//                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30L + 10L)));
                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(1L + 10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwdLearnPlayBean> reducedStream = windowedStream.reduce(
                new ReduceFunction<DwdLearnPlayBean>() {
                    @Override
                    public DwdLearnPlayBean reduce(DwdLearnPlayBean value1, DwdLearnPlayBean value2) throws Exception {
                        Integer playSec1 = value1.getPlaySec();
                        Integer playSec2 = value2.getPlaySec();
                        Long ts1 = value1.getTs();
                        Long ts2 = value2.getTs();

                        value1.setPlaySec(playSec1 + playSec2);
                        if (ts2 > ts1) {
                            value1.setPositionSec(value2.getPositionSec());
                        }

                        return value1;
                    }
                },
                new ProcessWindowFunction<DwdLearnPlayBean, DwdLearnPlayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwdLearnPlayBean> elements, Collector<DwdLearnPlayBean> out) throws Exception {
                        for (DwdLearnPlayBean element : elements) {
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 8. 转换数据结构
        SingleOutputStreamOperator<String> jsonStrStream = reducedStream.map(JSON::toJSONString);

        // TODO 9. 写出到 Kafka dwd_learn_play 主题
        String sinkTopic = "dwd_learn_play";
        jsonStrStream.addSink(
                KafkaUtil.getKafkaProducer(sinkTopic)
        );

        env.execute();
    }
}
