package com.qasr.protocol.config;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.qasr.protocol.APIClientPipelineFactory;
import com.qasr.protocol.transaction.TransactionObject;

public class QasrClient {
	private static ClientBootstrap bootstrap;
	public static void init(File r){
		try {
			Configure.load(r);
		} catch (Throwable e1) {
			e1.printStackTrace();
			System.exit(-1);
		}	
		bootstrap= new ClientBootstrap(new NioClientSocketChannelFactory( Executors.newCachedThreadPool(),Executors.newCachedThreadPool()));
		bootstrap.setOption("tcpNoDelay", Configure.getBoolProperty("bootstrap.tcpNoDelay"));
		bootstrap.setOption("keepAlive", Configure.getBoolProperty("bootstrap.keepAlive"));
		bootstrap.setOption("writeBufferHighWaterMark", Configure.getIntProperty("bootstrap.writeBufferHighWaterMark"));
		bootstrap.setOption("sendBufferSize", Configure.getIntProperty("bootstrap.sendBufferSize")); 
		bootstrap.setOption("receiveBufferSize", Configure.getIntProperty("bootstrap.receiveBufferSize"));
		bootstrap.setOption("connectTimeoutMillis", 1000);
	    bootstrap.setPipelineFactory(new APIClientPipelineFactory(null));
	}
	public static TransactionObject getTransacation(String dbname) throws IOException{
		return new TransactionObject(bootstrap, DBProxyInfo.getInfo(dbname));
	}
	public static void destroy() throws IOException{
		bootstrap.releaseExternalResources();
	}
}
