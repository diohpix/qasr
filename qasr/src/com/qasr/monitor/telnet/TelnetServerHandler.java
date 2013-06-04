package com.qasr.monitor.telnet;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qasr.db.LoadBalancer;
import com.qasr.db.ReloadableSqlSesseionFactoryBean;
import com.qasr.util.Configure;
import com.qasr.util.ResponseUtil;

public class TelnetServerHandler extends SimpleChannelUpstreamHandler {
	private final static Logger logger = LoggerFactory
			.getLogger(TelnetServerHandler.class);

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		if (e instanceof ChannelStateEvent) {
			logger.info(e.toString());
		}
		super.handleUpstream(ctx, e);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		// Send greeting for a new connection.
		e.getChannel().write("Welcome to " + InetAddress.getLocalHost().getHostName()+ "!\r\n");
		e.getChannel().write("It is " + new Date() + " now.\r\n");
	}

	public String plainText(){

		Iterator<String> names = Configure.getSQLFactoryNames();
		String a=new Date()+"\n";
		a+="name   \t\tactive\tidle \tmax\n";
		a+="-----------------------------------\n";
		
		while(names.hasNext()){
			String name = names.next();
			LoadBalancer lb;
			try {
				lb = Configure.getLoadBalancer(name);
				List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
				for (ReloadableSqlSesseionFactoryBean rs : connList) {
					SqlSessionFactory f =rs.getObject();
					BasicDataSource ds = (BasicDataSource)f.getConfiguration().getEnvironment().getDataSource();
					a+=String.format("%-16s%7d%7d%7d\n", name,ds.getNumActive(),ds.getNumIdle(),ds.getMaxActive());	
				}
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		}
		return a;
	}
	private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
	private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
	@SuppressWarnings("restriction")
	private static final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	private static long prevUptime=0;
	private static long prevCPUtime=0;
	private static int processor=0;
	static{
		processor = os.getAvailableProcessors();
		prevUptime = runtimeMXBean.getUptime();
		prevCPUtime = (long) os.getProcessCpuLoad();
	}
	public String json(){
		Map<String,Object> info =new HashMap<String,Object>(); 
		List<Map<String,Object>> data = new ArrayList<Map<String,Object>>();
		Iterator<String> names = Configure.getSQLFactoryNames();
		while(names.hasNext()){
			String name = names.next();
			LoadBalancer lb;
			try {
				lb = Configure.getLoadBalancer(name);
				List<ReloadableSqlSesseionFactoryBean> connList = lb.getSessionList();
				for (ReloadableSqlSesseionFactoryBean rs : connList) {
					SqlSessionFactory f =rs.getObject();
					BasicDataSource ds = (BasicDataSource)f.getConfiguration().getEnvironment().getDataSource();
					Map<String,Object> d = new HashMap<String,Object>();
					d.put("active", ds.getNumActive());
					d.put("name", name);
					d.put("idle", ds.getMaxIdle());
					d.put("max", ds.getMaxActive());
					data.add(d);
//					a+=String.format("%-16s%7d%7d%7d\n", name,ds.getNumActive(),ds.getNumIdle(),ds.getMaxActive());	
				}
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
		}
		long upTime = runtimeMXBean.getUptime();
	    long processCpuTime = threadBean.getCurrentThreadCpuTime();
	    long elapsedCpu = processCpuTime - prevCPUtime;
	    long elapsedTime = upTime - prevUptime;
	    double cpuUsage = Math.min(99F, elapsedCpu / (elapsedTime * 10000F * processor));
	    info.put("CPU" , cpuUsage);
	    info.put("VM" , os.getCommittedVirtualMemorySize());
	    info.put("TOTALM" , os.getTotalPhysicalMemorySize());
	    info.put("FREEM" , os.getFreePhysicalMemorySize());
		info.put("THREAD",threadBean.getThreadCount());
		info.put("date", new Timestamp(System.currentTimeMillis()));
		info.put("list",data);
		return ResponseUtil.getJSONString(info);
		
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

		// Cast to a String first.
		// We know it is a String because we put some codec in
		// TelnetPipelineFactory.
		String request = (String) e.getMessage();

		// Generate and write a response.
		String response;
		boolean close = false;
		if (request.length() == 0) {
			response = "Please type something.\r\n";
		} else if (request.toLowerCase().equals("bye")) {
			response = "Have a good day!\r\n";
			close = true;
		} else {
			response = "Did you say '" + request + "'?\r\n";
		}

		response =json();
		
		
		// We do not need to write a ChannelBuffer here.
		// We know the encoder inserted at TelnetPipelineFactory will do the
		// conversion.
		ChannelFuture future = e.getChannel().write(response);
		// Close the connection after sending 'Have a good day!'
		// if the client has sent 'bye'.
		if (close) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		logger.warn(null, e.getCause());
		e.getChannel().close();
	}
}
