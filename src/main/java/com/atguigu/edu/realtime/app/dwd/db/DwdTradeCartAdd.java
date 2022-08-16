package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_trade_cart_add";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 3. 筛选加购数据
        Table cartAdd = tableEnv.sqlQuery("" +
                "select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['session_id'] session_id,\n" +
                "data['cart_price'] cart_price,\n" +
                "ts\n" +
                "from `topic_db` \n" +
                "where `table` = 'cart_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("cart_add", cartAdd);

        // TODO 4. 建立 Kafka dwd_trade_cart_add 表
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "date_id string,\n" +
                "session_id string,\n" +
                "create_time string,\n" +
                "cart_price string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        // TODO 5. 将关联结果写入 Kafka
        tableEnv.executeSql("" +
                "insert into dwd_trade_cart_add select \n" +
                "id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "date_format(create_time, 'yyyy-MM-dd') date_id,\n" +
                "session_id,\n" +
                "create_time,\n" +
                "cart_price,\n" +
                "ts\n" +
                "from cart_add");
    }
}
