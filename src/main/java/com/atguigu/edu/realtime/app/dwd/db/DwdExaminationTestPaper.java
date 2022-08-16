package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdExaminationTestPaper {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_examination_test_paper";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 3. 筛选答卷表数据
        Table testExam = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['paper_id'] paper_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['score'] score,\n" +
                "data['duration_sec'] duration_sec,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['submit_time'] submit_time,\n" +
                "data['update_time'] update_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'test_exam'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("test_exam", testExam);

        // TODO 4. 创建 Kafka-Connector dwd_examination_test_paper 表
        tableEnv.executeSql("create table dwd_examination_test_paper(\n" +
                "id string,\n" +
                "paper_id string,\n" +
                "user_id string,\n" +
                "score string,\n" +
                "duration_sec string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "submit_time string,\n" +
                "update_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_examination_test_paper"));

        // TODO 5. 将数据写入 Kafka dwd_examination_test_paper 主题
        tableEnv.executeSql("insert into dwd_examination_test_paper\n" +
                "select * from test_exam");
    }
}
