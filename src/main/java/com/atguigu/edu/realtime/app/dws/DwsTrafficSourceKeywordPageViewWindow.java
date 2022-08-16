package com.atguigu.edu.realtime.app.dws;

import com.atguigu.edu.realtime.app.func.KeywordUDTF;
import com.atguigu.edu.realtime.bean.KeywordBean;
import com.atguigu.edu.realtime.common.EduConstant;
import com.atguigu.edu.realtime.util.ClickHouseUtil;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO 2. 从 Kafka dwd_traffic_page_log 主题中读取页面浏览日志数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page` map<string, string>,\n" +
                "`ts` bigint,\n" +
                "row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupId));

        // TODO 3. 从表中过滤搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] full_word,\n" +
                "row_time\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("search_table", searchTable);

        // TODO 4. 使用自定义的UDTF函数对搜索的内容进行分词
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "row_time \n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(full_word))\n" +
                "as t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 5. 分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                EduConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");

        // TODO 6. 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(KeywordBeanSearch, KeywordBean.class);

        // TODO 7. 将流中的数据写到ClickHouse中
        SinkFunction<KeywordBean> jdbcSink = ClickHouseUtil.<KeywordBean>getJdbcSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)");
        keywordBeanDS.addSink(jdbcSink);

        env.execute();
    }
}
