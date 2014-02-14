package com.khalifa.protocol.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import com.khalifa.protocol.ProtocolSelector;
import com.khalifa.util.CommonData;

/**
 * Receives a list of continent/city pairs from a {@link LocalTimeClient} to
 * get the local times of the specified cities.
 */
public class APIServer implements Runnable {

    private final int port;
    ServerBootstrap bootstrap = null;
    EventLoopGroup bossGroup =null;
    EventLoopGroup workerGroup = null;
    
    public APIServer(int port) {
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
        
        final DefaultEventExecutorGroup ev = new DefaultEventExecutorGroup(CommonData.executorGroupSize);
        /*final ProtobufEncoder protobufEncoder = new ProtobufEncoder();
        final CompressSelectEncoder compsel = new CompressSelectEncoder();
        final ProtobufVarint32LengthFieldPrepender protobufVar32 =  new ProtobufVarint32LengthFieldPrepender();
        final LengthEncoder lengthEncoder =new  LengthEncoder();
        
        final ProtobufDecoder protobufDecoder = new ProtobufDecoder(QueryProtocol.Query.getDefaultInstance());*/
        //final ProtocolSelector psel = new ProtocolSelector(ev);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline p = ch.pipeline();
			        p.addLast( new ReadTimeoutHandler(CommonData.readtimeout));
			        p.addLast( new ProtocolSelector(ev));
			        /*
			        p.addLast("LengthEncoder", lengthEncoder); //encoder
			        p.addLast( compsel); //encoder
			        p.addLast( protobufVar32); //encoder 
			        p.addLast( protobufEncoder); //encoder
			        

			        p.addLast(new ProtobufVarint32FrameDecoder()); //decoder
			        p.addLast( protobufDecoder); //decoder
			        p.addLast( ev,new ProtobufInboundHandler());
			        */
			}
		});
        bootstrap.localAddress(port);
        try{
        	ChannelFuture f = bootstrap.bind().sync();
        	f.channel().closeFuture().sync();
        }catch(Exception e){
        	shutdown();
        	System.out.println(e.getMessage());
        	System.exit(-1);
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
