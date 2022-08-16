package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTradePaySucWindowBean;
import com.atguigu.edu.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
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

import java.io.IOException;
import java.time.Duration;

public class DwsTradePaySucWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_trade_pay_suc_detail 主题读取数据
        String topic = "dwd_trade_pay_suc_detail";
        String groupId = "dws_trade_pay_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

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

        // TODO 6. 按照 user_id 分组
        KeyedStream<JSONObject, String> keyedStream = removedReplicatedStream.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // TODO 7. 统计支付成功独立用户数和新用户数
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, DwsTradePaySucWindowBean>() {

                    private ValueState<String> paySucLastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        paySucLastDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("pay_suc_last_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<DwsTradePaySucWindowBean> out) throws Exception {
                        long ts = jsonObj.getLong("ts") * 1000L;
                        String curDate = DateFormatUtil.toDate(ts);
                        String paySucLastDt = paySucLastDtState.value();
                        Long paySucUvCount = 0L;
                        Long paySucNewUserCount = 0L;

                        if (paySucLastDt == null) {
                            paySucUvCount = 1L;
                            paySucNewUserCount = 1L;
                            paySucLastDtState.update(curDate);
                        } else {
                            if (paySucLastDt.compareTo(curDate) < 0) {
                                paySucUvCount = 1L;
                                paySucLastDtState.update(curDate);
                            }
                        }
                        if (paySucUvCount != 0L) {
                            out.collect(DwsTradePaySucWindowBean.builder()
                                    .paySucUvCount(paySucUvCount)
                                    .paySucNewUserCount(paySucNewUserCount)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
        );

        // TODO 8. 设置水位线
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradePaySucWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradePaySucWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradePaySucWindowBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })
        );

        // TODO 9. 开窗
        AllWindowedStream<DwsTradePaySucWindowBean, TimeWindow> windowStream
                = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTradePaySucWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTradePaySucWindowBean>() {
                    @Override
                    public DwsTradePaySucWindowBean reduce(DwsTradePaySucWindowBean value1, DwsTradePaySucWindowBean value2) throws Exception {
                        value1.setPaySucUvCount(value1.getPaySucUvCount() + value2.getPaySucUvCount());
                        value1.setPaySucNewUserCount(value1.getPaySucNewUserCount() + value2.getPaySucNewUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsTradePaySucWindowBean, DwsTradePaySucWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTradePaySucWindowBean> elements, Collector<DwsTradePaySucWindowBean> out) throws Exception {
                        for (DwsTradePaySucWindowBean element : elements) {
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

        // TODO 11. 发送到 ClickHouse
        reducedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_pay_suc_window values(?,?,?,?,?)")
        );

        env.execute();
    }
}
