package com.khalifa.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.tomcat.dbcp.pool.impl.GenericKeyedObjectPool;
import org.apache.tomcat.dbcp.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.khalifa.db.LoadBalancer;
import com.khalifa.db.ReloadableSqlSesseionFactoryBean;
import com.khalifa.db.proxy.ProxyDataSource;
import com.khalifa.db.proxy.ProxySqlSessionFactoryBuilder;
import com.khalifa.protocol.server.APIServer;
import com.khalifa.util.CommonData;
import com.khalifa.util.Configure;

public class SystemInitializer {
	final static Logger logger = LoggerFactory.getLogger(SystemInitializer.class);	
	public static APIServer initAPIServer(){
		int api_servre_port = CommonData.api_server_port;
		 APIServer server = new APIServer(api_servre_port);
	        new Thread(server).start();
		return server;
	}
	
	public static void initConfigure(String [] args){
		ClassPathResource r = null;
    	if(args.length>0){
    		r = new ClassPathResource(Configure.CONFIG_ROOT+args[0]);
    	}else{
    		r = new ClassPathResource(Configure.CONFIG_ROOT+"khalifa.xml");
    	}
    	try {
			Configure.load(r.getFile());
		} catch (ConfigurationException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
	}
	public static void initDataSource(){
		List<HierarchicalConfiguration> cc =CommonData.jdbcs; 
		for (HierarchicalConfiguration jdbc : cc) {

        	List<String> urls = new ArrayList<String>();
        	
        	LoadBalancer balance = new LoadBalancer();
        	int size = jdbc.configurationsAt("urls.url").size();
        	if(size > 0){
        		List<HierarchicalConfiguration> l = jdbc.configurationsAt("urls.url");
        		for (HierarchicalConfiguration iurl : l) {
        			String h = (String) iurl.getRoot().getValue();
        			logger.info(h);
    				urls.add(h);
				}
        	}else{
        		String h = jdbc.getString("url");
            	logger.info(h);
        		urls.add(h);	
        	}
			String name = jdbc.getString("name");
        	String mybats_conf = jdbc.getString("mybatis-conf");
        	String mapper_mode = jdbc.getString("sqlmapper");
        	String username = (String) setDBCP(jdbc, "username", null);
        	String password = (String) setDBCP(jdbc, "password", null);
        	String driverClassName = (String) setDBCP(jdbc, "driverClassName", null);
        	logger.info("Initialize [{}]", name);
        	logger.info(driverClassName);

        	boolean defaultAutoCommit =setBoolDBCP(jdbc, "defaultAutoCommit", true) ;
    	    boolean defaultReadOnly = setBoolDBCP(jdbc, "defaultReadOnly", false) ;
    	    //int defaultTransactionIsolation = PoolableConnectionFactory.
    	    String defaultCatalog = (String) setDBCP(jdbc, "defaultCatalog", null) ;
    	    int maxActive =setIntDBCP(jdbc,"maxActive" , GenericObjectPool.DEFAULT_MAX_ACTIVE);
    	    int maxIdle = setIntDBCP(jdbc, "maxIdle", GenericObjectPool.DEFAULT_MAX_IDLE);
    	    int minIdle = setIntDBCP(jdbc, "minIdle",GenericObjectPool.DEFAULT_MIN_IDLE);
    	    int initialSize = setIntDBCP(jdbc, "initialSize",0);
    	    long maxWait = setLongDBCP(jdbc,"maxWait", GenericObjectPool.DEFAULT_MAX_WAIT);
    	    boolean poolPreparedStatements =setBoolDBCP(jdbc,"poolPreparedStatements", false);
    	    int maxOpenPreparedStatements = setIntDBCP(jdbc,"maxOpenPreparedStatements",GenericKeyedObjectPool.DEFAULT_MAX_TOTAL);
    	    boolean testOnBorrow =setBoolDBCP(jdbc,"testOnBorrow", true);
    	    boolean testOnReturn = setBoolDBCP(jdbc,"testOnReturn",false);
    	    long timeBetweenEvictionRunsMillis =  setLongDBCP(jdbc,"timeBetweenEvictionRunsMillis",GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    	    int numTestsPerEvictionRun = setIntDBCP(jdbc,"numTestsPerEvictionRun",GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN); 
    	    long minEvictableIdleTimeMillis =   setLongDBCP(jdbc,"minEvictableIdleTimeMillis",GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    	    boolean testWhileIdle =setBoolDBCP(jdbc,"testWhileIdle", false);
    	    String validationQuery =(String) setDBCP(jdbc, "validationQuery", null);
    	    int validationQueryTimeout = setIntDBCP(jdbc, "validationQueryTimeout", -1);


        	String mapperMode[] = mapper_mode.split(",");
        	for (String url : urls) {
        		
        		ProxyDataSource ds = new ProxyDataSource();
            	ds.setUsername(username);
            	ds.setPassword(password);
            	ds.setDriverClassName(driverClassName);
            	ds.setDefaultAutoCommit(defaultAutoCommit);
            	ds.setDefaultReadOnly(defaultReadOnly);
            	ds.setDefaultCatalog(defaultCatalog);
            	ds.setMaxActive(maxActive);
            	ds.setMaxIdle(maxIdle);
            	ds.setMinIdle(minIdle);
            	ds.setInitialSize(initialSize);
            	ds.setMaxWait(maxWait);
            	ds.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
            	ds.setTestOnBorrow(testOnBorrow);
            	ds.setTestOnReturn(testOnReturn);
            	ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
            	ds.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
            	ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
            	ds.setTestWhileIdle(testWhileIdle);
            	ds.setValidationQuery(validationQuery);
            	ds.setValidationQueryTimeout(validationQueryTimeout);
            	ds.setPoolPreparedStatements(poolPreparedStatements);
            	ds.setUrl(url);
            	ClassPathResource c = new ClassPathResource(Configure.CONFIG_ROOT+mybats_conf);
            	List<Resource> mapperR  = new ArrayList<Resource>();
            	for (String f : mapperMode) {
            		ClassPathResource cr = new ClassPathResource(Configure.CONFIG_ROOT+"mapper/"+f);
            		try {
            			if(cr.getFile().exists()){
            				if(cr.getFile().isFile()){
            					File  file = cr.getFile();
		        				if(file.getName().endsWith(".xml")){
		        					mapperR.add(new ClassPathResource(Configure.CONFIG_ROOT+"mapper/"+f));
		        				}
            				}else{
		        				File [] files = cr.getFile().listFiles();
		        				for (File file : files) {
		        					if(file.getName().endsWith(".xml")){
		        						mapperR.add(new ClassPathResource(Configure.CONFIG_ROOT+"mapper/"+mapper_mode+"/"+file.getName()));
		        					}
		        				}
            				}
            			}
        			} catch (IOException e) {
        				e.printStackTrace();
        			}	
				}
            	ReloadableSqlSesseionFactoryBean rsql = new ReloadableSqlSesseionFactoryBean(url);
            	rsql.setConfigLocation(c);
            	rsql.setMapperLocations(mapperR.toArray(new Resource[mapperR.size()]));
            	rsql.setDataSource(ds);
            	rsql.setSqlSessionFactoryBuilder(new ProxySqlSessionFactoryBuilder());
            	SqlSessionFactory s = rsql.getObject();
            	SqlSession ses  = s.openSession();
            	ses.close();
            	balance.addDataSource(rsql);
            	
			}
        	CommonData.addSQLFactory(name, balance);

        	
		}
	}
	private static Object setDBCP(HierarchicalConfiguration config,String key,Object defaultValue){
		if(config.containsKey(key)){
			if(defaultValue instanceof Boolean){
				return config.getBoolean(key);
			}
			return config.getProperty(key);
		}else{
			return defaultValue;
		}
	}
	private static int setIntDBCP(HierarchicalConfiguration config,String key,int defaultValue){
		if(config.containsKey(key)){
			return config.getInt(key);
		}else{
			return defaultValue;
		}
	}
	private static boolean setBoolDBCP(HierarchicalConfiguration config,String key,boolean defaultValue){
		if(config.containsKey(key)){
			return config.getBoolean(key);
		}else{
			return defaultValue;
		}
	}
	private static long setLongDBCP(HierarchicalConfiguration config,String key,long defaultValue){
		if(config.containsKey(key)){
			return config.getLong(key);
		}else{
			return defaultValue;
		}
	}
	
}
