package com.khalifa.config;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.File;
import java.io.IOException;

import com.khalifa.APIClientHandler;
import com.khalifa.LengthDecoder;
import com.khalifa.protocol.QueryProtocol;
import com.khalifa.transaction.TransactionObject;


public class DBProxyClient {
	private static Bootstrap bootstrap;
	private static EventLoopGroup workerGroup;
	public static void init(File r){
		try {
			Configure.load(r);
		} catch (Throwable e1) {
			e1.printStackTrace();
			System.exit(-1);
		}	
		workerGroup = new NioEventLoopGroup();
		bootstrap= new Bootstrap();
		bootstrap.group(workerGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.TCP_NODELAY, Configure.getBoolProperty("bootstrap.tcpNoDelay"));
		bootstrap.option(ChannelOption.SO_KEEPALIVE , Configure.getBoolProperty("bootstrap.keepAlive"));
		bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, Configure.getIntProperty("bootstrap.writeBufferHighWaterMark"));
		bootstrap.option(ChannelOption.SO_SNDBUF, Configure.getIntProperty("bootstrap.sendBufferSize")); 
		bootstrap.option(ChannelOption.SO_RCVBUF, Configure.getIntProperty("bootstrap.receiveBufferSize"));
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
		bootstrap.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				// TODO Auto-generated method stub
				ChannelPipeline p = ch.pipeline();
		        p.addLast( new ReadTimeoutHandler( 10));
		        p.addLast("LengthDecoder", new LengthDecoder(new JdkZlibDecoder(ZlibWrapper.ZLIB.GZIP))); //decoder
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder()); //decoder
		        p.addLast("protobufDecoder", new ProtobufDecoder(QueryProtocol.Response.getDefaultInstance())); //decoder
		        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender()); //encoder
		        p.addLast("protobufEncoder", new ProtobufEncoder()); //encoder
		        p.addLast("handler", new APIClientHandler());
		        
			}
		});
	    
	}
	public static TransactionObject getTransacation(String dbname) throws IOException{
		return new TransactionObject(bootstrap, DBProxyInfo.getInfo(dbname));
	}
	public static void destroy() throws IOException, InterruptedException{
		workerGroup.shutdownGracefully();
		workerGroup.terminationFuture().sync();
	}
}
