package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdUserUserRegister {
    public static void main(String[] args) {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 设置表状态 ttl
        String ttl = "10s";
        EnvUtil.setTableEnvStateTtl(tableEnv, ttl);

        // TODO 3. 从 Kafka 读取 topic_db 主题数据封装为表
        String groupId = "dwd_user_user_register";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 4. 从 Kafka dwd_traffic_page_log 主题读取数据，封装为表
        tableEnv.executeSql("create table page_log(" +
                "`common` map<String, String>,\n" +
                "`page` map<String, String>,\n" +
                "`ts` String\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log", groupId));

        // TODO 5. 筛选用户表数据
        Table userRegister = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['create_time'] register_time,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') register_date,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'user_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("user_register", userRegister);

        // TODO 6. 筛选日志中的维度信息
        Table dimInLog = tableEnv.sqlQuery("select \n" +
                "common['uid'] user_id,\n" +
                "common['ch'] channel,\n" +
                "common['ar'] province_id,\n" +
                "common['vc'] version_code,\n" +
                "common['sc'] source_id,\n" +
                "common['mid'] mid_id,\n" +
                "common['ba'] brand,\n" +
                "common['md'] model,\n" +
                "common['os'] operate_system\n" +
                "from page_log\n" +
                "where common['uid'] is not null");
        tableEnv.createTemporaryView("dim_in_log", dimInLog);

        // TODO 7. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "ur.id user_id,\n" +
                "ur.register_time,\n" +
                "ur.register_date,\n" +
                "log.channel,\n" +
                "log.province_id,\n" +
                "log.version_code,\n" +
                "log.source_id,\n" +
                "log.mid_id,\n" +
                "log.brand,\n" +
                "log.model,\n" +
                "log.operate_system,\n" +
                "ur.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from user_register ur\n" +
                "left join dim_in_log log\n" +
                "on ur.id = log.user_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 创建 Upsert-Kafka Connector dwd_user_user_register 表
        tableEnv.executeSql("create table dwd_user_user_register(\n" +
                "user_id string,\n" +
                "register_time string,\n" +
                "register_date string,\n" +
                "channel string,\n" +
                "province_id string,\n" +
                "version_code string,\n" +
                "source_id string,\n" +
                "mid_id string,\n" +
                "brand string,\n" +
                "model string,\n" +
                "operate_system string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(user_id) not enforced\n" +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_user_user_register"));

        // TODO 9. 将数据写入 Kafka dwd_user_user_register 主题
        tableEnv.executeSql("insert into dwd_user_user_register " +
                "select * from result_table");
    }
}
