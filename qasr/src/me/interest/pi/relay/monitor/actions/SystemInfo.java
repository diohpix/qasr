package me.interest.pi.relay.monitor.actions;

import java.util.Iterator;

import me.interest.pi.relay.util.Configure;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

public class SystemInfo implements ActionInterface{
	public String action(){
		Iterator<String> names = Configure.getSQLFactoryNames();
		System.out.println("------------------------------------------------------------------------------------------");
		String a ="";
		while(names.hasNext()){
			String name = names.next();
			SqlSessionFactory f;
			try {
				f = Configure.getSQLFactory(name);
				BasicDataSource ds = (BasicDataSource)f.getConfiguration().getEnvironment().getDataSource();
				a+=(name+" active "+ds.getNumActive()+" idle "+ds.getNumIdle()+" max "+ds.getMaxActive());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return a;
	}
}
