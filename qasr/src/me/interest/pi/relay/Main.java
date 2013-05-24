
package me.interest.pi.relay;

import java.util.concurrent.ThreadPoolExecutor;

import me.interest.pi.relay.protocol.server.APIServer;
import me.interest.pi.relay.system.SystemInitializer;
import me.interest.pi.relay.util.CommonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
