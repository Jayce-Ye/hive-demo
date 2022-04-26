package com.alan;

import java.sql.*;

public class JDBCUtils {
    private static String driver="org.apache.hive.jdbc.HiveDriver";
    private static String url="jdbc:hive2://node-etl-01:10000/default";

    //注册驱动
    static{
        try{
            Class.forName(driver);
        }catch(ClassNotFoundException e){
            throw new ExceptionInInitializerError(e);
        }
    }
    //获取连接
    public static Connection getConnection(){
        try{
            return DriverManager.getConnection(url, "root", "");
        }catch(SQLException e){
            e.printStackTrace();
        }
        return null;
    }
    //释放资源
    public static void release(Connection conn, Statement st, ResultSet rs){
        if(conn!=null){
            try{
                conn.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                conn=null;
            }
        }

        if(st!=null){
            try{
                st.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                st=null;
            }
        }

        if(rs!=null){
            try{
                rs.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                rs=null;
            }
        }


    }
}