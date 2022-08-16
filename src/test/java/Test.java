import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.EduConfig;
import com.atguigu.edu.realtime.util.DruidDSUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/6/14
 */
public class Test {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        JSONObject jsonObj = new JSONObject();
//        jsonObj.put("a", "1");
//        jsonObj.put("b", "2");
//        filter(jsonObj, "a");
//        System.out.println(jsonObj);

        DruidPooledConnection conn = DruidDSUtil.createDataSource().getConnection();
//
//        conn.prepareStatement("upsert into EDU_REALTIME.test(id,name) values('2', '大纲')").executeUpdate();
//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//        Connection conn = DriverManager.getConnection(EduConfig.PHOENIX_SERVER);
        PreparedStatement preparedStatement = conn.prepareStatement("upsert into EDU_REALTIME.test(id,name) values(?, ?)");
        preparedStatement.setString(1, "9 = 'ha'");
        preparedStatement.setString(2, "'9 = 'ha''");
        preparedStatement.execute();
//        conn.commit();
    }

    private static void filter(JSONObject jsonObj, String sinkColumns) {
//        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
//        entrySet.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }
}
