package com.jjf.jdbc;

import java.util.Properties;

public class UrlFactory {
	
	private static final String JJF_DRIVER = "jdbc:jjf://";
	
	public static String getUrl(String url, Properties info) {
		if(null != url && url.regionMatches(true, 0, JJF_DRIVER, 0, JJF_DRIVER.length())) {
			url = url.replaceAll("jjf", "mysql");
		}
		return url;
	}
}
