package com.atguigu.edu.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

/**
 * description:
 * Created by 铁盾 on 2022/6/14
 */
public class PhoenixUtil {
    public static void executeDDL(DruidPooledConnection conn, String ddlSql) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(ddlSql);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("建表时获取数据库操作对象异常");
        }

        try {
            preparedStatement.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("建表语句执行异常");
        }

        close(conn, preparedStatement);
    }

    /**
     * DML 语句执行方法，通过预编译传参的方式避免 SQL 注入现象
     * @param conn 连接对象
     * @param sinkTable 目标表
     * @param jsonObj 数据对象
     */
    public static void executeDML(DruidPooledConnection conn, String sinkTable, JSONObject jsonObj) {
        Set<Map.Entry<String, Object>> entries = jsonObj.entrySet();
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        StringBuilder symbols = new StringBuilder();

        for (Map.Entry<String, Object> entry : entries) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
            symbols.append("?,");
        }

        String columnStr = StringUtils.join(columns, ",");
        String symbolStr = symbols.substring(0, symbols.length() - 1).toString();

        StringBuilder insertSql =
                new StringBuilder("upsert into " + EduConfig.HBASE_SCHEMA + "." + sinkTable + "(");
        insertSql
                .append(columnStr)
                .append(") values(")
                .append(symbolStr)
                .append(")");
        String sql = insertSql.toString();

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            for (int i = 0; i < values.size(); i++) {
                preparedStatement.setObject(i + 1, values.get(i) + "");
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("数据库操作对象获取或传参异常");
        }

        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("DML语句执行异常");
        }

        close(conn, preparedStatement);
    }

    /**
     * Phoenix 表查询方法
     * @param conn 数据库连接对象
     * @param sql 查询数据的 SQL 语句
     * @param clz 返回的集合元素类型的 class 对象
     * @param <T> 返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |

             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()){
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    private static void close(DruidPooledConnection conn, PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("数据库操作对象归还异常");
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("连接对象归还异常");
            }
        }
    }
}
