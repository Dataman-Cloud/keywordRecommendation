package com.dataman.webservice.io;

import java.sql.Connection;
import java.sql.SQLException;



import java.sql.Statement;

import com.dataman.webservice.util.MyConnectionManager;

public class MsgDAO {
	
	public static void inser(InputMsg msgin) throws SQLException{
		Connection conn = null;
		Statement st = null;
		try {
			conn = MyConnectionManager.getConnection();
			st = conn.createStatement();
			st.executeUpdate(msgin.toInsertSql());
		} catch (SQLException e) {
			throw e;
		}finally{
			MyConnectionManager.free(conn, st, null);
		}
	}
}
