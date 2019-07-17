package big13.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Spark SQL分布式查询引擎
 */
public class SparlSQLThriftServerDemo {
    public static void main(String[] args) throws Exception {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driver);
        String url = "jdbc:hive1://192.168.52.154:10000";
        Connection connection = DriverManager.getConnection(url);
        PreparedStatement ppst = connection.prepareStatement("select * from usr;");
        ResultSet rs = ppst.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            System.out.println(id + "-" + name + "-" + age);
        }
        rs.close();
        ppst.close();
        connection.close();
    }
}
