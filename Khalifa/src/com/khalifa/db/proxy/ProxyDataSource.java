package com.khalifa.db.proxy;

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

public class ProxyDataSource extends BasicDataSource {

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
	public void reset(){
		try {
			close();
			closed=false;
			createDataSource();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
