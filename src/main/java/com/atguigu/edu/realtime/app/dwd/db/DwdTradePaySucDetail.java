package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradePaySucDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 设置表状态 ttl 及时区
        String ttl = (15 * 60 + 5) + "s";
        EnvUtil.setTableEnvStateTtl(tableEnv, ttl);
        // 设置 FlinkSQL 计算时的时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_trade_pay_suc_detail";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 4. 读取 Kafka dwd_trade_order_detail 主题数据，封装为 Flink SQL 表
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
                "row_op_ts TIMESTAMP_LTZ(3) " +
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail", groupId));

        // TODO 5. 筛选支付成功数据
        Table paymentSuc = tableEnv.sqlQuery("select\n" +
                "data['alipay_trade_no'] alipay_trade_no,\n" +
                "data['trade_body'] trade_body,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['payment_status'] payment_status,\n" +
                "data['callback_time'] callback_time,\n" +
                "data['callback_content'] callback_content,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n" +
//                "and `type` = 'update'\n" +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_suc", paymentSuc);

        // TODO 6. 关联 2 张表获得支付成功宽表
        Table resultTable = tableEnv.sqlQuery("" +
                "select \n" +
                "id,\n" +
                "od.order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "alipay_trade_no,\n" +
                "pay_suc.trade_body,\n" +
                "payment_type,\n" +
                "payment_status,\n" +
                "callback_time,\n" +
                "callback_content,\n" +
                "origin_amount,\n" +
                "coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "pay_suc.ts ts,\n" +
                "row_op_ts\n" +
                "from payment_suc pay_suc\n" +
                "join dwd_trade_order_detail od\n" +
                "on pay_suc.order_id = od.order_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 创建 Upsert-Kafka dwd_trade_pay_suc_detail 表
        tableEnv.executeSql("create table dwd_trade_pay_suc_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "alipay_trade_no string,\n" +
                "trade_body string,\n" +
                "payment_type string,\n" +
                "payment_status string,\n" +
                "callback_time string,\n" +
                "callback_content string,\n" +
                "original_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "ts string,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_suc_detail"));

        // TODO 8. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_pay_suc_detail select * from result_table");
    }
}
