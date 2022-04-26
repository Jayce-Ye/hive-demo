package com.alan;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestHive {
    public static void main(String[] args) {
        // 注册jdbc驱动
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            // 创建连接
            Connection conn = DriverManager.getConnection("jdbc:hive2://node-etl-01:10000/default", "root", "");
            // 创建SQL执行器
            Statement st = conn.createStatement();
            // 执行SQL语句，得到结果集
            String sql = "select * from word_count";
            ResultSet rs = st.executeQuery(sql);
            // 处理结果
            while (rs.next()) {
                System.out.println(rs.getString("word") + " " + rs.getInt("count"));
            }
            // 关闭资源
            rs.close();
            st.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
