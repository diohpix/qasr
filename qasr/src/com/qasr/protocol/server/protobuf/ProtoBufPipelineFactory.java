package com.qasr.protocol.server.protobuf;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.compression.ZlibWrapper;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;

import org.jboss.netty.util.Timer;

import com.qasr.protocol.QueryProtocol;
import com.qasr.util.Configure;

public class ProtoBufPipelineFactory implements ChannelPipelineFactory {
/*
	private ProtobufVarint32LengthFieldPrepender	  frmaeEncoder = new ProtobufVarint32LengthFieldPrepender();
	private ProtobufEncoder protobufEncoder = new ProtobufEncoder();
	private ZlibEncoder zlibencoder = new ZlibEncoder(ZlibWrapper.GZIP);
	private ZlibDecoder zlibdecoder = new ZlibDecoder(ZlibWrapper.GZIP);
	
	private ProtobufVarint32FrameDecoder protobufFrameDecoder = new ProtobufVarint32FrameDecoder();
	private ProtobufDecoder protobufDecoder = new ProtobufDecoder(QueryProtocol.Query.getDefaultInstance());
	
	*/
	private ChannelHandler readTimeoutHandler;
	private Timer timer = null;
	private final ExecutionHandler exehandle;
	public ProtoBufPipelineFactory(ExecutionHandler exe){
		this.exehandle = exe;
		this.timer = new HashedWheelTimer();
		this.readTimeoutHandler = new ReadTimeoutHandler(timer, 30);
	}
	
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();
        p.addLast("readTimeHandler", this.readTimeoutHandler);
        
        p.addLast("inflater", new ZlibDecoder(ZlibWrapper.GZIP));
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(QueryProtocol.Query.getDefaultInstance()));
        
        
        p.addLast("deflater", new ZlibEncoder(ZlibWrapper.GZIP));
        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        
        p.addLast("protobufEncoder", new ProtobufEncoder());        
        
        
        if(Configure.useExecuteHandler){
        	p.addLast("exehandler", this.exehandle);
        }
        p.addLast("handler", new ProtoBufHandler());
        return p;
    }
}
