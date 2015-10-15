package com.jjf.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDriverMain {
	public static void main(String[] args) throws SQLException {
		String url = "jdbc:jjf://localhost:3306/test";
        Properties info = new Properties();
        info.setProperty("user", "root");
        info.setProperty("password", "123456");
        Connection con = null;
        try {
            con = DriverManager.getConnection(url, info);
            Statement stmt = con.createStatement();
            String query = "select id,member_id from t1 limit 1";
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println("id:" + rs.getString(1) + ",member_id:" + rs.getString(2));
            }
            rs.close();
            stmt.close();
        } finally {
            if (con != null) {
                con.close();
            }
        }
	}
}
