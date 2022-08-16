package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdExaminationTestQuestion {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_examination_test_question";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 3. 筛选答卷问题表数据
        Table testExamQuestion = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['paper_id'] paper_id,\n" +
                "data['question_id'] question_id,\n" +
                "data['answer'] answer,\n" +
                "data['is_correct'] is_correct,\n" +
                "data['score'] score,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['update_time'] update_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'test_exam_question'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("test_exam_question", testExamQuestion);

        // TODO 4. 创建 Kafka-Connector dwd_examination_test_question 表
        tableEnv.executeSql("create table dwd_examination_test_question(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "paper_id string,\n" +
                "question_id string,\n" +
                "answer string,\n" +
                "is_correct string,\n" +
                "score string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "update_time string,\n" +
                "ts string" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_examination_test_question"));

        // TODO 5. 将数据写入 Kafka dwd_examination_test_question 主题
        tableEnv.executeSql("insert into dwd_examination_test_question\n" +
                "select * from test_exam_question");
    }
}
