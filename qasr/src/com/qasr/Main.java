
package com.qasr;

import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qasr.protocol.server.APIServer;
import com.qasr.system.SystemInitializer;
import com.qasr.util.CommonObject;

public class Main {
	final static Logger logger = LoggerFactory.getLogger(Main.class);
    private static APIServer server = null;
    public static void main(String args[]){
    	logger.debug("debug");
    	
    	SystemInitializer.initConfigure(args);
    	SystemInitializer.initDataSource();
    	SystemInitializer.initExecuteHandler();
        try{
	        server =SystemInitializer.initAPIServer();
	        SystemInitializer.initMonitorServer();
	        logger.info("Context Listener > Initialized");
	        new Thread(new com.qasr.monitor.telnet.TelnetSocketServer(10001)).start();
        }catch(Exception e){
        	if(server!=null){
        		server.shutdown();
        	}
        	if(CommonObject.executor!=null){
        		((ThreadPoolExecutor)CommonObject.executor).shutdown();
        	}
        	System.out.println(e.getCause().getMessage());
        	System.exit(-1);
        }
    }
    
    public void destroy() {
    	if(server!=null){
    		server.shutdown();
    	}
    	if(CommonObject.executor!=null){
    		((ThreadPoolExecutor)CommonObject.executor).shutdown();
    	}
    	System.out.println("Complete");
    }
}
