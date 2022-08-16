package com.atguigu.edu.realtime.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * description:
 * Created by 铁盾 on 2022/6/13
 */
public class EduConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "EDU_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
//    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:zookeeper.bigdata:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
//    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/edu_realtime";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://chi-clickhouse-simple-0-0:8123/edu_realtime";

}
