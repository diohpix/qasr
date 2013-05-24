package com.qasr.monitor.actions;

import java.util.Iterator;


import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

import com.qasr.util.Configure;

public class ConnectionPoolMonitor implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				Thread.sleep(1000);
				Iterator<String> names = Configure.getSQLFactoryNames();
				System.out.println("------------------------------------------------------------------------------------------");
				while(names.hasNext()){
					String name = names.next();
					SqlSessionFactory f = Configure.getSQLFactory(name);
					BasicDataSource ds = (BasicDataSource)f.getConfiguration().getEnvironment().getDataSource();
					System.out.println(name+" active "+ds.getNumActive()+" idle "+ds.getNumIdle()+" max "+ds.getMaxActive());
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

}
