package com.jjf.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.NonRegisteringDriver;

public class JDriver extends NonRegisteringDriver implements Driver{

	
	static {
        try {
            java.sql.DriverManager.registerDriver(new JDriver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }
	
	
	public JDriver() throws SQLException {
		super();
	}
	
	@Override
    public Connection connect(String url, Properties info) throws SQLException {
        return super.connect(UrlFactory.getUrl(url, info), info);
    }

}
