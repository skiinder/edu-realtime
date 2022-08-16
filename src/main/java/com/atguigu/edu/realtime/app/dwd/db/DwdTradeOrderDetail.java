package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 设置表状态 ttl 及时区
        String ttl = "10s";
        EnvUtil.setTableEnvStateTtl(tableEnv, ttl);
        // 设置 FlinkSQL 计算时的时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_trade_order_detail";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 4. 从 Kafka 读取日志数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table page_log(" +
                "`common` map<String, String>,\n" +
                "`page` map<String, String>,\n" +
                "`ts` String\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log", groupId));

        // TODO 5. 筛选订单明细表数据
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['origin_amount'] origin_amount,\n" +
                "data['coupon_reduce'] coupon_reduce,\n" +
                "data['final_amount'] final_amount,\n" +
                "ts\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // TODO 6. 筛选订单表数据
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "data['out_trade_no'] out_trade_no,\n" +
                "data['session_id'] session_id,\n" +
                "data['trade_body'] trade_body\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 7. 筛选订单页面日志，获取 source_id
        Table filteredLog = tableEnv.sqlQuery("select " +
                "common['sid'] session_id,\n" +
                "common['sc'] source_id\n" +
                "from page_log\n" +
                "where page['page_id'] = 'order'");
        tableEnv.createTemporaryView("filter_log", filteredLog);

        // TODO 8. 关联三张表获得订单明细表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "oi.session_id,\n" +
                "source_id,\n" +
                "create_time,\n" +
                "origin_amount,\n" +
                "coupon_reduce coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "out_trade_no,\n" +
                "trade_body,\n" +
                "ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join filter_log fl\n" +
                "on oi.session_id = fl.session_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 9. 建立 Upsert-Kafka dwd_trade_order_detail 表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "session_id string,\n" +
                "source_id string,\n" +
                "create_time string,\n" +
                "origin_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "out_trade_no string,\n" +
                "trade_body string,\n" +
                "ts string,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        // TODO 10. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_detail \n" +
                "select * from result_table");
    }
}
