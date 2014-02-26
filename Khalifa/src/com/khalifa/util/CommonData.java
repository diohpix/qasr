package com.khalifa.util;

import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.khalifa.db.LoadBalancer;
import com.khalifa.process.State;

public class CommonData {
	public static final AttributeKey<State> STATE =  AttributeKey.valueOf("request.state");
	
	public final static Logger timeoutLogger = LoggerFactory.getLogger("TIMEOUT");
	public final static Logger exceptionLogger = LoggerFactory.getLogger("EXCEPTION");
	
	private static Map<String,LoadBalancer> sqlfactory = new HashMap<String, LoadBalancer>(); 
	
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
	
	public static boolean monitorEnable=false;
	public static boolean monitorUseDBProxyPort=false;
	public static int monitor_port=0;
	public static int mapper_check_interval;
	public static int slow_query_time;
	public static int redis_default_expire;
	public static int readtimeout;
	public static int executorGroupSize;
	
	public static int api_server_port;
	public static int api_server_writeBufferHighWaterMark;
	public static int api_server_sendBufferSize;
	public static int api_server_receiveBufferSize;
	public static int api_server_backlog;
	public static boolean api_server_tcpNoDelay;
	public static boolean api_server_keepAlive;
	
	public static int api_child_sendBufferSize;
	public static int api_child_receiveBufferSize;
	public static boolean api_child_tcpNoDelay;
	public static int api_child_soLinger;
	
	public static int redis_maxActive;
	public static int redis_maxIdle;
	public static int redis_maxWait;
	public static boolean redis_testOnBorrow;
	public static boolean redis_testWhileIdle;
	public static int redis_evictableIdleTimeMillis;
	public static int redis_timeBetweenEvictionRunsMillis;
	public static List<HierarchicalConfiguration> redis_servers_host;
	public static List<HierarchicalConfiguration> jdbcs ;
}
