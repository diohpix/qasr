package com.khalifa.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.khalifa.db.LoadBalancer;
import com.khalifa.db.ReloadableSqlSesseionFactoryBean;
import com.khalifa.db.proxy.ProxyDataSource;
import com.khalifa.db.proxy.ProxySqlSessionFactoryBuilder;
import com.khalifa.protocol.server.APIServer;
import com.khalifa.util.Configure;

public class SystemInitializer {
	final static Logger logger = LoggerFactory.getLogger(SystemInitializer.class);	
	public static APIServer initAPIServer(){
		int api_servre_port = Configure.getIntProperty("api.server.port");
		 APIServer server = new APIServer(api_servre_port);
	        new Thread(server).start();
		return server;
	}
	
	public static void initConfigure(String [] args){
		ClassPathResource r = null;
    	if(args.length>0){
    		r = new ClassPathResource(Configure.CONFIG_ROOT+args[0]);
    	}else{
    		r = new ClassPathResource(Configure.CONFIG_ROOT+"dbproxy.xml");
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
        List<Object> ss = Configure.getList("jdbcs.item.name");
        int jdbclen = ss.size();
        for(int i = 0 ; i < jdbclen ; i++){
        	String name = Configure.getProperty("jdbcs.item("+i+").name");
        	String driver = Configure.getProperty("jdbcs.item("+i+").driver");
        	logger.info("Initialize [{}]", name);
        	logger.info(driver);
        	
        	int k=0;
        	List<String> urls = new ArrayList<String>();
        	
        	
        	LoadBalancer balance = new LoadBalancer();

        	if((Configure.getProperty("jdbcs.item("+i+").urls.url")).length()>0){
	        	while(true){
	    			String h = Configure.getProperty("jdbcs.item("+i+").urls.url("+k+")");
	    			if("".equals(h)){
	    				break;
	    			}else{
	    	        	logger.info(h);
	    				urls.add(h);
	    			}
	    			k++;
	    		}
        	}else{
        		String h = Configure.getProperty("jdbcs.item("+i+").url");
            	logger.info(h);
        		urls.add(h);	
        	}
        	
        	String mybats_conf = Configure.getProperty("jdbcs.item("+i+").mybatis-conf");
        	String mapper_mode = Configure.getProperty("jdbcs.item("+i+").sqlmapper");
        	String username = Configure.getProperty("jdbcs.item("+i+").user");
        	String password = Configure.getProperty("jdbcs.item("+i+").password");
        	boolean autocomm = Configure.getBoolProperty("jdbcs.item("+i+").defaultAutoCommit");
        	
        	String mapperMode[] = mapper_mode.split(",");
        	for (String url : urls) {
        		ProxyDataSource ds = new ProxyDataSource();
            	ds.setUsername(username);
            	ds.setPassword(password);
            	ds.setDriverClassName(driver);
            	ds.setDefaultAutoCommit(autocomm);
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
        	Configure.addSQLFactory(name, balance);
        }

	}
}
