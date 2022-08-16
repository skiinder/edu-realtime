package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.common.EduConfig;
import com.atguigu.edu.realtime.util.DruidDSUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/6/13
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject>{

    MapStateDescriptor<String, DimTableProcess> tableProcessDescriptor;

    DruidDataSource druidDataSource;

    public MyBroadcastFunction(MapStateDescriptor<String, DimTableProcess> tableProcessDescriptor) {
        this.tableProcessDescriptor = tableProcessDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, DimTableProcess> tableProcessState = readOnlyContext.getBroadcastState(tableProcessDescriptor);
        // 获取配置信息
        String sourceTable = jsonObj.getString("table");
        DimTableProcess tableProcess = tableProcessState.get(sourceTable);

        if(tableProcess != null) {
            // 判断操作类型是否为 null，校验数据结构是否完整
            String type = jsonObj.getString("type");
            if(type == null) {
                System.out.println("Maxwell 采集的数据格式异常，缺少操作类型");
            } else {
                JSONObject data = jsonObj.getJSONObject("data");

                String sinkTable = tableProcess.getSinkTable();
                String sinkPk = tableProcess.getSinkPk();
                String sinkColumns = tableProcess.getSinkColumns();

                // 根据 sinkColumns 过滤数据
                filter(data, sinkColumns);

                // 将目标表名加入到主流数据中
                data.put("sink_table", sinkTable);
                data.put("type", type);
                data.put("sink_pk", sinkPk);

                collector.collect(data);
            }
        }
    }

    private void filter(JSONObject jsonObj, String sinkColumns) {
        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        entrySet.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        BroadcastState<String, DimTableProcess> tableProcessState = context.getBroadcastState(tableProcessDescriptor);

        String op = jsonObj.getString("op");

        if("d".equals(op)) {
            JSONObject before = jsonObj.getJSONObject("before");
            String sourceTable = before.getString("source_table");
            tableProcessState.remove(sourceTable);
        } else if (op == null) {
            System.out.println("FlinkCDC 采集到的数据格式有误，缺少操作类型");
        } else {
            JSONObject config = jsonObj.getJSONObject("after");
            String sourceTable = config.getString("source_table");
            String sinkTable = config.getString("sink_table");
            String sinkColumns = config.getString("sink_columns");
            String sinkPk = config.getString("sink_pk");
            String sinkExtend = config.getString("sink_extend");

            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

            DimTableProcess tableProcess = DimTableProcess.builder()
                    .sourceTable(sourceTable)
                    .sinkTable(sinkTable)
                    .sinkColumns(sinkColumns)
                    .sinkPk(sinkPk)
                    .sinkExtend(sinkExtend)
                    .build();

            tableProcessState.put(sourceTable, tableProcess);
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        StringBuilder createSql = new StringBuilder("create table if not exists " + EduConfig.HBASE_SCHEMA + "." + sinkTable);
        String[] columnArr = sinkColumns.split(",");
        if(sinkPk == null) {
            sinkPk = "id";
        }
        if(sinkExtend == null){
            sinkExtend = "";
        }
        createSql.append("(");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            createSql.append(column).append(" varchar");
            if(column.equals(sinkPk)) {
                createSql.append(" primary key");
            }
            if(i < columnArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")").append(sinkExtend);

        String sql = createSql.toString();

        DruidPooledConnection conn = null;

        try {
            conn = druidDataSource.getConnection();
        } catch (Exception exception) {
            exception.printStackTrace();
            System.out.println("从连接池获取连接对象异常");
        }

        PhoenixUtil.executeDDL(conn, sql);
    }
}
