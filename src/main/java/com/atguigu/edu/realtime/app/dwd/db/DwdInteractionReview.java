package com.atguigu.edu.realtime.app.dwd.db;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionReview {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        String groupId = "dwd_interaction_review";
        KafkaUtil.createTopicDb(tableEnv, groupId);

        // TODO 3. 读取课程评价表数据
        Table reviewInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['review_txt'] review_txt,\n" +
                "data['review_stars'] review_stars,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'review_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("review_info", reviewInfo);

        // TODO 4. 建立 Kafka-Connector dwd_interaction_review 表
        tableEnv.executeSql("create table dwd_interaction_review(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "review_txt string,\n" +
                "review_stars string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_review"));

        // TODO 5. 将评论表数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_review select * from review_info");
    }
}
