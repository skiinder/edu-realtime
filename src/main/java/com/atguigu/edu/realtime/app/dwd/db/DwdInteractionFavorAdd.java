package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionFavorAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_interaction_favor_add";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 3. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'favor_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 4. 创建 Kafka-Connector dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_favor_add"));

        // TODO 5. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info");
    }
}
