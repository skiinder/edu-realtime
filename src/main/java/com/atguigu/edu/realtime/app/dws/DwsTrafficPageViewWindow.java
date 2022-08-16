package com.atguigu.edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwsTrafficPageViewWindowBean;
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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 从 Kafka dwd_traffic_page_log 主题读取数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构，筛选首页、课程列表页、课程详情页浏览记录
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> filteredStream = source.process(
                new ProcessFunction<String, DwsTrafficPageViewWindowBean>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");
                        String mid = common.getString("mid");
                        String pageId = page.getString("page_id");
                        if (pageId.equals("home") ||
                                pageId.equals("course_list") ||
                                pageId.equals("course_detail")) {
                            out.collect(DwsTrafficPageViewWindowBean.builder()
                                    .mid(mid)
                                    .pageId(pageId)
                                    .ts(ts)
                                    .build());
                        }

                    }
                }
        );

        // TODO 4. 按照 mid 分组
        KeyedStream<DwsTrafficPageViewWindowBean, String> keyedStream = filteredStream.keyBy(DwsTrafficPageViewWindowBean::getMid);

        // TODO 5. 统计各页面独立访客数
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTrafficPageViewWindowBean, DwsTrafficPageViewWindowBean>() {

                    private ValueState<String> homeLastDtState;
                    private ValueState<String> courseListLastDtState;
                    private ValueState<String> courseDetailLastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> homeLastDtStateDescriptor =
                                new ValueStateDescriptor<>("home_last_dt_state", String.class);
                        homeLastDtStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build());
                        this.homeLastDtState = getRuntimeContext().getState(homeLastDtStateDescriptor);

                        ValueStateDescriptor<String> courseListLastDtStateDescriptor =
                                new ValueStateDescriptor<>("course_list_last_dt_state", String.class);
                        courseListLastDtStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build()
                        );
                        this.courseListLastDtState = getRuntimeContext().getState(courseListLastDtStateDescriptor);

                        ValueStateDescriptor<String> courseDetailLastDtStateDescriptor =
                                new ValueStateDescriptor<>("course_detail_last_dt_state", String.class);
                        courseDetailLastDtStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).build()
                        );
                        this.courseDetailLastDtState = getRuntimeContext().getState(courseDetailLastDtStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTrafficPageViewWindowBean bean, Context context, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {

                        bean.setHomeUvCount(0L);
                        bean.setListUvCount(0L);
                        bean.setDetailUvCount(0L);

                        String pageId = bean.getPageId();
                        String curDate = DateFormatUtil.toDate(bean.getTs());
                        switch (pageId) {
                            case "home":
                                String homeLastDt = homeLastDtState.value();
                                if (homeLastDt == null ||
                                        homeLastDt.compareTo(curDate) < 0) {
                                    bean.setHomeUvCount(1L);
                                    homeLastDtState.update(curDate);
                                    out.collect(bean);
                                }
                                break;
                            case "course_list":
                                String courseListLastDt = courseListLastDtState.value();
                                if (courseListLastDt == null ||
                                        courseListLastDt.compareTo(curDate) < 0) {
                                    bean.setListUvCount(1L);
                                    courseListLastDtState.update(curDate);
                                    out.collect(bean);
                                }
                                break;
                            case "course_detail":
                                String courseDetailLastDt = courseDetailLastDtState.value();
                                if (courseDetailLastDt == null ||
                                        courseDetailLastDt.compareTo(curDate) < 0) {
                                    bean.setDetailUvCount(1L);
                                    courseDetailLastDtState.update(curDate);
                                    out.collect(bean);
                                }
                                break;
                        }
                    }
                }
        );

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTrafficPageViewWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTrafficPageViewWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTrafficPageViewWindowBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // TODO 7. 开窗
        AllWindowedStream<DwsTrafficPageViewWindowBean, TimeWindow> windowStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> reducedStream = windowStream.reduce(
                new ReduceFunction<DwsTrafficPageViewWindowBean>() {
                    @Override
                    public DwsTrafficPageViewWindowBean reduce(DwsTrafficPageViewWindowBean value1, DwsTrafficPageViewWindowBean value2) throws Exception {
                        value1.setHomeUvCount(value1.getHomeUvCount() + value2.getHomeUvCount());
                        value1.setListUvCount(value1.getListUvCount() + value2.getListUvCount());
                        value1.setDetailUvCount(value1.getDetailUvCount() + value2.getDetailUvCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<DwsTrafficPageViewWindowBean, DwsTrafficPageViewWindowBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTrafficPageViewWindowBean> elements, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (DwsTrafficPageViewWindowBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 9. 写出到 ClickHouse
        reducedStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into dws_traffic_page_view_window values(?,?,?,?,?,?)"));

        env.execute();
    }
}
