package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DimUtil;
import com.atguigu.edu.realtime.util.DruidDSUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class MyPhoenixSink extends RichSinkFunction<JSONObject> {

    static DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取操作类型
        String type = jsonObj.getString("type");

        // 获取目标表名
        String sinkTable = jsonObj.getString("sink_table");
        // 获取主键字段名称
        String sinkPk = jsonObj.getString("sink_pk");

        // 清除描述数据的字段，
        jsonObj.remove("type");
        jsonObj.remove("sink_table");
        jsonObj.remove("sink_pk");

        // 获取连接对象
        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("获取连接对象异常");
        }

        PhoenixUtil.executeDML(conn, sinkTable, jsonObj);

        if (type.equals("update")) {
            String sinkPkValue = jsonObj.getString(sinkPk);
            DimUtil.deleteCached(sinkTable, sinkPkValue);
        }
    }
}
