package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradeCartAddWindowBean;
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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_cart_add 主题读取数据
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_add_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 按照 user_id 分组
        KeyedStream<String, String> keyedStream = source.keyBy(
                jsonStr -> JSON.parseObject(jsonStr).getString("user_id")
        );

        // TODO 4. 统计加购独立用户数
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, String, DwsTradeCartAddWindowBean>() {

                    private ValueState<String> lastCartAddDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> lastCartAddDtStateDescriptor
                                = new ValueStateDescriptor<>("last_cart_add_dt_state", String.class);
                        lastCartAddDtStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1L)).build()
                        );
                        this.lastCartAddDtState = getRuntimeContext().getState(lastCartAddDtStateDescriptor);
                    }

                    @Override
                    public void processElement(String jsonStr, Context context, Collector<DwsTradeCartAddWindowBean> out) throws Exception {
                        String lastCartAddDt = lastCartAddDtState.value();
                        String curDate = DateFormatUtil.toDate(JSON.parseObject(jsonStr).getLong("ts") * 1000L);
                        if (lastCartAddDt == null || lastCartAddDt.compareTo(curDate) < 0) {
                            lastCartAddDtState.update(curDate);
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            long ts = jsonObj.getLong("ts") * 1000L;
                            DwsTradeCartAddWindowBean bean = DwsTradeCartAddWindowBean.builder()
                                    .cartAddUvCount(1L)
                                    .ts(ts)
                                    .build();
                            out.collect(bean);
                        }
                    }
                }
        );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCartAddWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCartAddWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeCartAddWindowBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })
        );

        // TODO 6. 开窗
        AllWindowedStream<DwsTradeCartAddWindowBean, TimeWindow> windowStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTradeCartAddWindowBean>() {
                    @Override
                    public DwsTradeCartAddWindowBean reduce(DwsTradeCartAddWindowBean value1, DwsTradeCartAddWindowBean value2) throws Exception {
                        value1.setCartAddUvCount(value1.getCartAddUvCount() + value2.getCartAddUvCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradeCartAddWindowBean> elements, Collector<DwsTradeCartAddWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTradeCartAddWindowBean element : elements) {
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
                ClickHouseUtil.getJdbcSink("insert into dws_trade_cart_add_window values(?,?,?,?)")
        );

        env.execute();
    }
}
