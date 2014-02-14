package com.khalifa.util;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configure {
	private final static Logger log = LoggerFactory.getLogger(Configure.class);
	public static String CONFIG_ROOT="../conf/";
	public static boolean useCacheGzip = false;
	
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
		CommonData.mapper_check_interval = Configure.getIntProperty("mapper_check_interval");
		CommonData.slow_query_time = Configure.getIntProperty("slow_query_time");
		CommonData.readtimeout = Configure.getIntProperty("readtimeout");
		CommonData.executorGroupSize = Configure.getIntProperty("executorGroupSize");
		
		CommonData.api_server_port = Configure.getIntProperty("api.server.port");
		CommonData.api_server_writeBufferHighWaterMark = Configure.getIntProperty("api.server.writeBufferHighWaterMark");
		CommonData.api_server_sendBufferSize= Configure.getIntProperty("api.server.sendBufferSize");
		CommonData.api_server_receiveBufferSize = Configure.getIntProperty("api.server.receiveBufferSize");
		CommonData.api_server_backlog = Configure.getIntProperty("api.server.backlog");
		CommonData.api_child_sendBufferSize = Configure.getIntProperty("api.child.sendBufferSize");
		CommonData.api_child_receiveBufferSize = Configure.getIntProperty("api.child.receiveBufferSize");
		CommonData.api_child_tcpNoDelay = Configure.getBoolProperty("api.child.tcpNoDelay");
		CommonData.api_child_soLinger = Configure.getIntProperty("api.child.soLinger");
		CommonData.api_server_tcpNoDelay = Configure.getBoolProperty("api.server.tcpNoDelay");
		CommonData.api_server_keepAlive= Configure.getBoolProperty("api.server.keepAlive");
		
		CommonData.redis_maxActive = Configure.getIntProperty("redis.maxActive");
		CommonData.redis_maxIdle = Configure.getIntProperty("redis.maxIdle");
		CommonData.redis_maxWait = Configure.getIntProperty("redis.maxWait");
		CommonData.redis_testOnBorrow = Configure.getBoolProperty("redis.testOnBorrow");
		CommonData.redis_testWhileIdle = Configure.getBoolProperty("redis.testWhileIdle");
		CommonData.redis_evictableIdleTimeMillis =Configure.getIntProperty("redis.evictableIdleTimeMillis");
		CommonData.redis_timeBetweenEvictionRunsMillis = Configure.getIntProperty("redis.timeBetweenEvictionRunsMillis");
		CommonData.redis_servers_host = Configure.configurationsAt("redis.servers.host");
		CommonData.redis_default_expire = Configure.getIntProperty("redis.default_expire");
		
		CommonData.jdbcs =  Configure.configurationsAt("jdbcs.item");
	}
	private static List<HierarchicalConfiguration>  configurationsAt(String key){
		return config.configurationsAt(key);
	}
	
	
	private static int getIntProperty(String key) {
		return config.getInt(key);
	}
	public static boolean getBoolProperty(String key){
		
		return config.getBoolean(key);
	}
	private static List<Object> getList(String key){
		return config.getList(key);
	}
}
