package com.khalifa.db.proxy;

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import org.apache.tomcat.dbcp.dbcp2.BasicDataSource;

import ch.qos.logback.core.joran.spi.DefaultClass;

public class ProxyDataSource extends BasicDataSource {

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
	@Deprecated
	public void reset(){
		try {
			close();
//			closed=false;
			createDataSource();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
