package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsUserLoginWindowBean;
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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserLoginWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_user_user_login 主题读取数据
        String topic = "dwd_user_user_login";
        String groupId = "dws_user_login_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 按照 userId 分组
        KeyedStream<String, String> keyedStream = source.keyBy(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    return jsonObj.getString("userId");
                }
        );

        // TODO 4. 统计回流用户数和独立用户数
        SingleOutputStreamOperator<DwsUserLoginWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, String, DwsUserLoginWindowBean>() {

                    private ValueState<String> lastLoginDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastLoginDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_login_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(String jsonStr, Context context, Collector<DwsUserLoginWindowBean> out) throws Exception {
                        Long ts = JSON.parseObject(jsonStr).getLong("ts");
                        String lastLoginDt = lastLoginDtState.value();
                        String curDate = DateFormatUtil.toDate(ts);
                        Long uvCount = 0L;
                        Long backCount = 0L;
                        if (lastLoginDt == null) {
                            lastLoginDtState.update(curDate);
                            uvCount = 1L;
                        } else {
                            if (lastLoginDt.compareTo(curDate) < 0) {
                                lastLoginDtState.update(curDate);
                                uvCount = 1L;
                                if ((DateFormatUtil.toTs(curDate)) - DateFormatUtil.toTs(lastLoginDt) / 1000 / 3600 / 24 >= 8) {
                                    backCount = 1L;
                                }
                            }
                        }
                        if (uvCount != 0L || backCount != 0L) {
                            DwsUserLoginWindowBean bean = DwsUserLoginWindowBean.builder()
                                    .backCount(backCount)
                                    .uvCount(uvCount)
                                    .ts(ts)
                                    .build();
                            out.collect(bean);
                        }
                    }
                }
        );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsUserLoginWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsUserLoginWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsUserLoginWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsUserLoginWindowBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        // TODO 6. 开窗
        AllWindowedStream<DwsUserLoginWindowBean, TimeWindow> windowStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsUserLoginWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsUserLoginWindowBean>() {
                    @Override
                    public DwsUserLoginWindowBean reduce(DwsUserLoginWindowBean value1, DwsUserLoginWindowBean value2) throws Exception {
                        value1.setBackCount(value1.getBackCount() + value2.getBackCount());
                        value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsUserLoginWindowBean, DwsUserLoginWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsUserLoginWindowBean> elements, Collector<DwsUserLoginWindowBean> out) throws Exception {
                        for (DwsUserLoginWindowBean element : elements) {
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

        // TODO 8. 写出到 ClickHouse
        reducedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_user_login_window values(?,?,?,?,?)")
        );

        env.execute();
    }
}
