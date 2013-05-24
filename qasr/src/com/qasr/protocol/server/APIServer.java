package com.qasr.protocol.server;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.qasr.protocol.server.bytetrans.IntHeaderPipelineFactory;
import com.qasr.protocol.server.protobuf.ProtoBufPipelineFactory;
import com.qasr.util.Configure;

/**
 * Receives a list of continent/city pairs from a {@link LocalTimeClient} to
 * get the local times of the specified cities.
 */
public class APIServer implements Runnable {

    private final int port;
    private ExecutorService e1=null;
    private ExecutorService e2=null;
    ServerBootstrap bootstrap = null;
    public APIServer(int port) {
        this.port = port;
    }

    public void run() {
        // Configure the server.
    	e1= Executors.newCachedThreadPool();
    	e2 = Executors.newCachedThreadPool();
        bootstrap = new ServerBootstrap(  new NioServerSocketChannelFactory( e1,e2 ));

        // Set up the event pipeline factory.
        bootstrap.setOption("tcpNoDelay", Configure.getBoolProperty("api.server.tcpNoDelay"));
        bootstrap.setOption("writeBufferHighWaterMark",Configure.getIntProperty("api.server.writeBufferHighWaterMark"));
        bootstrap.setOption("sendBufferSize",Configure.getIntProperty("api.server.sendBufferSize"));
        bootstrap.setOption("receiveBufferSize",Configure.getIntProperty("api.server.receiveBufferSize"));
        bootstrap.setOption("backlog", Configure.getIntProperty("api.server.backlog"));
        //bootstrap.setOption("soLinger",0);
        bootstrap.setOption("keepAlive",Configure.getBoolProperty("api.server.keepAlive"));
        
        bootstrap.setOption("child.sendBufferSize",Configure.getIntProperty("api.child.sendBufferSize"));
        bootstrap.setOption("child.receiveBufferSize",Configure.getIntProperty("api.child.receiveBufferSize"));
        bootstrap.setOption("child.tcpNoDelay", Configure.getBoolProperty("api.child.tcpNoDelay"));
        bootstrap.setOption("child.soLinger",Configure.getIntProperty("api.child.soLinger"));
        
       
        if(Configure.useExecuteHandler){
        	System.out.println("USE EXECUTION HANDLER");
        }
        if(Configure.useProtoBuf){
        	System.out.println("USE PROTOCOL BUFFER");
        	if(Configure.useExecuteHandler){
        		 ExecutionHandler exehandler = new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(500,1048576,1048576));
        		bootstrap.setPipelineFactory(new ProtoBufPipelineFactory(exehandler));
        	}else{
        		bootstrap.setPipelineFactory(new ProtoBufPipelineFactory(null));
        	}
        }else{
        	bootstrap.setPipelineFactory(new IntHeaderPipelineFactory());
        }
        try{
        	bootstrap.bind(new InetSocketAddress(port));
        }catch(Exception e){
        	shutdown();
        	System.out.println(e.getCause().getMessage());
        	System.exit(-1);
        }
    }
    public void shutdown(){
    	e1.shutdownNow();
    	e2.shutdownNow();
    	bootstrap.shutdown();
    }
}
