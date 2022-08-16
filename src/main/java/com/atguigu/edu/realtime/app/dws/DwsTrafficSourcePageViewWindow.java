package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTrafficSourcePageViewWindowBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * description:
 * Created by 铁盾 on 2022/6/20
 */
@Deprecated
public class DwsTrafficSourcePageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_traffic_page_log 主题读取数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 按照 mid 和 sourceId 分组
        KeyedStream<String, String> keyedStream = source.keyBy(
                jsonStr -> {
                    JSONObject common = JSON.parseObject(jsonStr).getJSONObject("common");
                    String mid = common.getString("mid");
                    String sourceId = common.getString("sc");
                    return mid + sourceId;
                });

        // TODO 4. 转换数据结构，记录独立访客数
        SingleOutputStreamOperator<DwsTrafficSourcePageViewWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, String, DwsTrafficSourcePageViewWindowBean>() {

                    private ValueState<String> lastVisitDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastVisitDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_visit_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(String jsonStr, Context context, Collector<DwsTrafficSourcePageViewWindowBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String sourceId = jsonObj.getJSONObject("common").getString("sc");
                        String lastVisitDt = lastVisitDtState.value();
                        String curDate = DateFormatUtil.toDate(System.currentTimeMillis());
                        if (lastVisitDt == null || !lastVisitDt.equals(curDate)) {
                            lastVisitDtState.update(curDate);
                            DwsTrafficSourcePageViewWindowBean bean = DwsTrafficSourcePageViewWindowBean.builder()
                                    .sourceId(sourceId)
                                    .uvCt(1L)
                                    .build();
                            out.collect(bean);
                        }
                    }
                }
        );

        // TODO 5. 按照 sourceId 分组
        KeyedStream<DwsTrafficSourcePageViewWindowBean, String> sourceIdKeyedStream = processedStream.keyBy(DwsTrafficSourcePageViewWindowBean::getSourceId);

        // TODO 5. 开窗
        WindowedStream<DwsTrafficSourcePageViewWindowBean, String, TimeWindow> windowStream = sourceIdKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10L)));

        // TODO 6. 聚合
        SingleOutputStreamOperator<DwsTrafficSourcePageViewWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTrafficSourcePageViewWindowBean>() {
                    @Override
                    public DwsTrafficSourcePageViewWindowBean reduce(DwsTrafficSourcePageViewWindowBean value1, DwsTrafficSourcePageViewWindowBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTrafficSourcePageViewWindowBean, DwsTrafficSourcePageViewWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTrafficSourcePageViewWindowBean> elements, Collector<DwsTrafficSourcePageViewWindowBean> out) throws Exception {
                        for (DwsTrafficSourcePageViewWindowBean element : elements) {
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

        // TODO 7. 关联获取来源名称字段
        SingleOutputStreamOperator<DwsTrafficSourcePageViewWindowBean> finalStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsTrafficSourcePageViewWindowBean>("dim_base_source".toUpperCase()) {
                    @Override
                    public void join(DwsTrafficSourcePageViewWindowBean bean, JSONObject dimJsonObj) throws Exception {
                        String sourceName = dimJsonObj.getString("source_site".toUpperCase());
                        bean.setSourceName(sourceName);
                    }

                    @Override
                    public String getKey(DwsTrafficSourcePageViewWindowBean bean) {
                        return bean.getSourceId();
                    }
                },
                60 * 5,
                TimeUnit.SECONDS
        );

        // TODO 8. 写出到 ClickHouse
        finalStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_traffic_source_page_view_window values(?,?,?,?,?,?)")
        );

        env.execute();

    }
}
