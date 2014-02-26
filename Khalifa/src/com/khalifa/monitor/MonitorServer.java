package com.khalifa.monitor;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;

import com.khalifa.protocol.server.http.HttpSnoopServerHandler;
import com.khalifa.util.CommonData;

/**
 * Receives a list of continent/city pairs from a {@link LocalTimeClient} to
 * get the local times of the specified cities.
 */
public class MonitorServer implements Runnable {

    private final int port;
    ServerBootstrap bootstrap = null;
    EventLoopGroup bossGroup =null;
    EventLoopGroup workerGroup = null;
    
    public MonitorServer(int port) {
        this.port = port;
    }

    public void run() {
        // Configure the server.
    	
    	bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        // Set up the event pipeline factory.
        bootstrap.option(ChannelOption.TCP_NODELAY, CommonData.api_server_tcpNoDelay);
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,CommonData.api_server_writeBufferHighWaterMark);
        bootstrap.option(ChannelOption.SO_SNDBUF,CommonData.api_server_sendBufferSize);
        bootstrap.option(ChannelOption.SO_RCVBUF,CommonData.api_server_receiveBufferSize);
        bootstrap.option(ChannelOption.SO_BACKLOG, CommonData.api_server_backlog);
        bootstrap.option(ChannelOption.SO_LINGER ,0);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE,CommonData.api_server_keepAlive);
        
        bootstrap.childOption(ChannelOption.SO_SNDBUF,CommonData.api_child_sendBufferSize);
        bootstrap.childOption(ChannelOption.SO_RCVBUF,CommonData.api_child_receiveBufferSize);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, CommonData.api_child_tcpNoDelay);
        bootstrap.childOption(ChannelOption.SO_LINGER,CommonData.api_child_soLinger);
        
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline p = ch.pipeline();
			        p.addLast( new ReadTimeoutHandler(CommonData.readtimeout));
			        p.addLast("decoder", new HttpRequestDecoder());
			        p.addLast("encoder", new HttpResponseEncoder());
			        p.addLast("deflater", new HttpContentCompressor());
			        p.addLast("handler", new HttpSnoopServerHandler());
			}
		});
        bootstrap.localAddress(port);
        try{
        	ChannelFuture f = bootstrap.bind().sync();
        	f.channel().closeFuture().sync();
        }catch(Exception e){
        	shutdown();
        	System.out.println(e.getMessage());
        }finally{
        	shutdown();
        }
    }
    public void shutdown(){
    	bossGroup.shutdownGracefully();
    	workerGroup.shutdownGracefully();
    	try {
			bossGroup.terminationFuture().sync();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			workerGroup.terminationFuture().sync();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
