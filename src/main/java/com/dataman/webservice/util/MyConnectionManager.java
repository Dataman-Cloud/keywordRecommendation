package com.dataman.webservice.util;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class MyConnectionManager {
	
	private static DataSource dataSource;  
    
    private MyConnectionManager(){}  
    
    static {
        try {  
            //Class.forName("com.mysql.jdbc.Driver");  
        	System.out.println(".....initing connection pool");
            Properties pro = new Properties();  
            InputStream in = MyConnectionManager.class.getClassLoader().getResourceAsStream("db.properties");  
            pro.load(in);  
            dataSource = BasicDataSourceFactory.createDataSource(pro);
            System.out.println(".....connection pool inited");
        } catch (Exception e) {  
            throw new ExceptionInInitializerError(e);  
        }  
    }  
  
    public static Connection getConnection() throws SQLException {  
        return dataSource.getConnection();  
    }  
  
    public static void free(Connection con, Statement st, ResultSet rs) {  
        try {  
            if (rs != null)  
                rs.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                if (st != null)  
                    st.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            } finally {  
                if (con != null)  
                    try {  
                        con.close();  
                    } catch (SQLException e) {  
                        e.printStackTrace();  
                    }  
            }  
        }  
    }
}
