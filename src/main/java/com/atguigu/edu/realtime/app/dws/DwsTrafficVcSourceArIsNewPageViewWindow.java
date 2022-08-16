package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.DimAsyncFunction;
import com.atguigu.edu.realtime.bean.DwsTrafficForSourcePvBean;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.DateFormatUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
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

public class DwsTrafficVcSourceArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 读取 Kafka page_log 主题数据，封装为表
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_source_ar_is_new_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> mainStream = source.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");
                        return    DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .ar(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(0L)
                                .totalSessionCount(
                                        page.getString("last_page_id") == null ? 1L : 0L)
                                .pageViewCount(1L)
                                .totalDuringTime(
                                        page.getLong("during_time"))
                                .jumpSessionCount(0L)
                                .ts(ts)
                                .build();

                    }
                }
        );

        // TODO 4. 从 Kafka dwd_traffic_unique_vistor_detail 主题读取跳出明细数据
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String uvGroupId = "dws_traffic_vc_source_ar_is_new_page_view_window";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getKafkaConsumer(uvTopic, uvGroupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);

        // TODO 5. 转换数据结构，记录独立访客数
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> uvStream = uvSource.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObj.getJSONObject("common");
                        Long ts = jsonObj.getLong("ts");

                        return DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .ar(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(1L)
                                .totalSessionCount(0L)
                                .pageViewCount(0L)
                                .totalDuringTime(0L)
                                .jumpSessionCount(0L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        // TODO 6. 从 Kafka dwd_traffic_user_jump_detail 主题读取跳出明细数据
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String ujdGroupId = "dws_traffic_vc_source_ar_is_new_page_view_window";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getKafkaConsumer(ujdTopic, ujdGroupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);

        // TODO 7. 转换数据结构，记录跳出明细
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> ujdStream = ujdSource.map(
                new MapFunction<String, DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObj.getJSONObject("common");
                        Long ts = jsonObj.getLong("ts");

                        return DwsTrafficForSourcePvBean.builder()
                                .versionCode(common.getString("vc"))
                                .sourceId(common.getString("sc"))
                                .ar(common.getString("ar"))
                                .isNew(common.getString("is_new"))
                                .uvCount(0L)
                                .totalSessionCount(0L)
                                .pageViewCount(0L)
                                .totalDuringTime(0L)
                                .jumpSessionCount(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        // TODO 8. 合并三条流
        DataStream<DwsTrafficForSourcePvBean> unionStream = mainStream
                .union(uvStream)
                .union(ujdStream);

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withWatermarkStream = unionStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTrafficForSourcePvBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTrafficForSourcePvBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTrafficForSourcePvBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // TODO 10. 分组
        KeyedStream<DwsTrafficForSourcePvBean, String> keyedStream = withWatermarkStream.keyBy(
                new KeySelector<DwsTrafficForSourcePvBean, String>() {
                    @Override
                    public String getKey(DwsTrafficForSourcePvBean bean) throws Exception {
                        return bean.getVersionCode() +
                                bean.getSourceId() +
                                bean.getSourceName() +
                                bean.getAr() +
                                bean.getProvinceName() +
                                bean.getIsNew();
                    }
                }
        );

        // TODO 11. 开窗
        WindowedStream<DwsTrafficForSourcePvBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean reduce(DwsTrafficForSourcePvBean value1, DwsTrafficForSourcePvBean value2) throws Exception {
                        value1.setPageViewCount(value1.getPageViewCount() + value2.getPageViewCount());
                        value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                        value1.setJumpSessionCount(value1.getJumpSessionCount() + value2.getJumpSessionCount());
                        value1.setTotalDuringTime(value1.getTotalDuringTime() + value2.getTotalDuringTime());
                        value1.setTotalSessionCount(value1.getTotalSessionCount() + value2.getTotalSessionCount());

                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTrafficForSourcePvBean> elements, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        long ts = System.currentTimeMillis();

                        for (DwsTrafficForSourcePvBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(ts);
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 13. 维度关联
        // 13.1 补充省份名称
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withProvinceNameStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<DwsTrafficForSourcePvBean>("dim_base_province".toUpperCase()) {
                    @Override
                    public void join(DwsTrafficForSourcePvBean bean, JSONObject dimJsonObj) throws Exception {
                        String provinceName = dimJsonObj.getString("name".toUpperCase());
                        bean.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(DwsTrafficForSourcePvBean bean) {
                        return bean.getAr();
                    }
                },
                60 * 5, TimeUnit.MINUTES
        );

        // 13.2 补充来源名称
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withSourceNameStream = AsyncDataStream.unorderedWait(
                withProvinceNameStream,
                new DimAsyncFunction<DwsTrafficForSourcePvBean>("dim_base_source".toUpperCase()) {
                    @Override
                    public void join(DwsTrafficForSourcePvBean bean, JSONObject dimJsonObj) throws Exception {
                        String sourceName = dimJsonObj.getString("source_site".toUpperCase());
                        bean.setSourceName(sourceName);
                    }

                    @Override
                    public String getKey(DwsTrafficForSourcePvBean bean) {
                        return bean.getSourceId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // TODO 14. 写出到 ClickHouse
        withSourceNameStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into dws_traffic_vc_source_ar_is_new_page_view_window " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
