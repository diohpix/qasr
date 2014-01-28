package com.cj.enm.platform.dbproxy.util;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cj.enm.platform.dbproxy.db.LoadBalancer;

public class Configure {
	private final static Logger log = LoggerFactory.getLogger(Configure.class);
	public static String CONFIG_ROOT="../conf/";
	public static boolean useCacheGzip = false;
	private static Map<String,LoadBalancer> sqlfactory = new HashMap<String, LoadBalancer>(); 
	private static  XMLConfiguration config ;
	public static void load(File fileName) throws ConfigurationException {
		XMLConfiguration.setDefaultListDelimiter('|');
		config = new XMLConfiguration(fileName);
		if(log.isDebugEnabled()){
			
			System.out.println("================================================================");
			System.out.println("System Configure");
			System.out.println("Config fileName = " + fileName);
			System.out.println("================================================================");
			Iterator<String> key = config.getKeys();
			while(key.hasNext()){
				String k = key.next();
				System.out.println(k+"="+config.getString(k));
			}
			System.out.println("================================================================");
		}
		
	}
	public static Iterator<String> getSQLFactoryNames(){
		return sqlfactory.keySet().iterator();
	}
	public static SqlSessionFactory getSQLFactory(String name) throws Exception{
		SqlSessionFactory sf = sqlfactory.get(name).getSession();
		if(sf ==null) throw new Exception("SqlSession "+name+" not found");
		return sf;
	}
	public static LoadBalancer getLoadBalancer(String name) throws Exception{
		LoadBalancer sf = sqlfactory.get(name);
		if(sf ==null) throw new Exception("LoadBalancer "+name+" not found");
		return sf;
	}
	public static void addSQLFactory(String name,LoadBalancer value){
		sqlfactory.put(name, value);
	}
	public static String getProperty(String key) {
		String value = config.getProperty(key) == null ? ""
				: config.getProperty(key).toString();
		return value;
	}
	public static int getIntProperty(String key) {
		return config.getInt(key);
	}
	public static boolean getBoolProperty(String key){
		
		return config.getBoolean(key);
	}
	public static Object getObject(String key){
		return config.getProperty(key);
	}
	public static List<Object> getList(String key){
		return config.getList(key);
	}
}
