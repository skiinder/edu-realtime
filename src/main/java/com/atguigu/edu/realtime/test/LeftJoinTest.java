package com.atguigu.edu.realtime.test;

import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * description:
 * Created by 铁盾 on 2022/6/29
 */
public class LeftJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvUtil.setTableEnvStateTtl(tableEnv, "10 s");

        tableEnv.executeSql("create table left_table(\n" +
                "id String,\n" +
                "tag String\n" +
                ")" + KafkaUtil.getKafkaDDL("left_table", "leftJoinTest"));

        tableEnv.executeSql("create table right_table(\n" +
                "id String,\n" +
                "tag String\n" +
                ")" + KafkaUtil.getKafkaDDL("right_table", "leftJoinTest"));

        Table result_table = tableEnv.sqlQuery("select \n" +
                "l.id l_id,\n" +
                "l.tag l_tag,\n" +
                "r.tag r_tag\n" +
                "from left_table l \n" +
                "left join \n" +
                "right_table r \n" +
                "on l.id = r.id\n");

        tableEnv.createTemporaryView("result_table", result_table);

        tableEnv.executeSql("create table upsert_left_join(\n" +
                "l_id String,\n" +
                "l_tag String,\n" +
                "r_tag String,\n" +
                "primary key(l_id) not enforced" +
                ")" + KafkaUtil.getUpsertKafkaDDL("upsert_left_join"));

        tableEnv.executeSql("insert into upsert_left_join\n" +
                "select * from result_table");
    }
}
