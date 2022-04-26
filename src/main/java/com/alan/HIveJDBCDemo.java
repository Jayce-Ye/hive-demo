package com.alan;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class HIveJDBCDemo {
    public static void main(String[] args){
//        Connection conn=null;
//        Statement st=null;
//        ResultSet rs=null;
        String sql ="select * from word_count";
        try{
            //获取连接
            Connection conn=JDBCUtils.getConnection();
            //创建运行环境
            assert conn != null;
            Statement st = conn.createStatement();
            //运行HQL

            ResultSet rs = st.executeQuery(sql);
            //处理数据
            while(rs.next()){
                String  name=rs.getString("word");
                System.out.println(name);
            }
            rs.close();
            st.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}