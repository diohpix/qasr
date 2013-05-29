package com.qasr.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.qasr.db.LoadBalancer;
import com.qasr.db.ReloadableSqlSesseionFactoryBean;
import com.qasr.monitor.http.WebSocketServer;
import com.qasr.protocol.server.APIServer;
import com.qasr.util.CommonObject;
import com.qasr.util.Configure;


public class SystemInitializer {
	final static Logger logger = LoggerFactory.getLogger(SystemInitializer.class);	
	public static APIServer initAPIServer(){
		int api_servre_port = Configure.getIntProperty("api.server.port");
		 APIServer server = new APIServer(api_servre_port);
	        new Thread(server).start();
		return server;
	}
	public static void initMonitorServer(){
		int monitor_servre_port = Configure.getIntProperty("monitor.port");
		WebSocketServer http = new WebSocketServer(monitor_servre_port);
        new Thread(http).start();
		
	}
	public static void initExecuteHandler(){
		Configure.useExecuteHandler = Configure.getBoolProperty("useExecuteHandler");
        int worker_queue_size = Configure.getIntProperty("worker.queue_size");
        int worker_min_thread = Configure.getIntProperty("worker.min_thread");
        int worker_max_thread = Configure.getIntProperty("worker.max_thread");
        if(!Configure.useExecuteHandler){
        	CommonObject.queue = new LinkedBlockingQueue<Runnable>(worker_queue_size);
        	CommonObject.executor =	new ThreadPoolExecutor(worker_min_thread, worker_max_thread, 50000L,	TimeUnit.MILLISECONDS,	CommonObject.queue);
        }
		
	}
	public static void initConfigure(String [] args){
		ClassPathResource r = null;
    	if(args.length>0){
    		r = new ClassPathResource(Configure.CONFIG_ROOT+args[0]);
    	}else{
    		r = new ClassPathResource(Configure.CONFIG_ROOT+"qasr.xml");
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
		Configure.useExecuteHandler = Configure.getBoolProperty("useExecuteHandler");
        List<Object> ss = Configure.getList("jdbcs.item.name");
        int jdbclen = ss.size();
        for(int i = 0 ; i < jdbclen ; i++){
        	String name = Configure.getProperty("jdbcs.item("+i+").name");
        	String driver = Configure.getProperty("jdbcs.item("+i+").driver");
        	
        	int k=0;
        	List<String> urls = new ArrayList<String>();
        	
        	
        	LoadBalancer balance = new LoadBalancer();

        	if((Configure.getProperty("jdbcs.item("+i+").urls.url")).length()>0){
	        	while(true){
	    			String h = Configure.getProperty("jdbcs.item("+i+").urls.url("+k+")");
	    			if("".equals(h)){
	    				break;
	    			}else{
	    				urls.add(h);
	    				logger.info(h);
	    			}
	    			k++;
	    		}
        	}else{
        		urls.add(Configure.getProperty("jdbcs.item("+i+").url"));	
        	}
        	
        	String mybats_conf = Configure.getProperty("jdbcs.item("+i+").mybatis-conf");
        	String mapper_mode = Configure.getProperty("jdbcs.item("+i+").sqlmapper");
        	String username = Configure.getProperty("jdbcs.item("+i+").user");
        	String password = Configure.getProperty("jdbcs.item("+i+").password");
        	boolean autocomm = Configure.getBoolProperty("jdbcs.item("+i+").defaultAutoCommit");
        	
        	
        	for (String url : urls) {
        		BasicDataSource ds = new BasicDataSource();
            	ds.setUsername(username);
            	ds.setPassword(password);
            	ds.setDriverClassName(driver);
            	ds.setDefaultAutoCommit(autocomm);
            	ds.setUrl(url);
            	ClassPathResource c = new ClassPathResource(Configure.CONFIG_ROOT+mybats_conf);
            	ClassPathResource cr = new ClassPathResource(Configure.CONFIG_ROOT+"mapper/"+mapper_mode);
            	List<Resource> mapperR  = new ArrayList<Resource>(); 
            	try {
    				File [] file = cr.getFile().listFiles();
    				for (File file2 : file) {
    					if(file2.getName().endsWith(".xml")){
    						mapperR.add(new ClassPathResource(Configure.CONFIG_ROOT+"mapper/"+mapper_mode+"/"+file2.getName()));
    					}
    				}
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
            	
            	ReloadableSqlSesseionFactoryBean rsql = new ReloadableSqlSesseionFactoryBean();
            	rsql.setConfigLocation(c);
            	
            	rsql.setMapperLocations(mapperR.toArray(new Resource[mapperR.size()]));
            	rsql.setDataSource(ds);
            	balance.addDataSource(rsql);
            	
			}
        	Configure.addSQLFactory(name, balance);
        }

	}
}
