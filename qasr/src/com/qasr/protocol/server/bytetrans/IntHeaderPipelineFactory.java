package com.qasr.protocol.server.bytetrans;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

public class IntHeaderPipelineFactory implements ChannelPipelineFactory {
/*
	private ProtobufVarint32LengthFieldPrepender	  frmaeEncoder = new ProtobufVarint32LengthFieldPrepender();
	private ProtobufEncoder protobufEncoder = new ProtobufEncoder();
	private ZlibEncoder zlibencoder = new ZlibEncoder(ZlibWrapper.GZIP);
	private ZlibDecoder zlibdecoder = new ZlibDecoder(ZlibWrapper.GZIP);
	
	private ProtobufVarint32FrameDecoder protobufFrameDecoder = new ProtobufVarint32FrameDecoder();
	private ProtobufDecoder protobufDecoder = new ProtobufDecoder(QueryProtocol.Query.getDefaultInstance());
	
	*/
	
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();
        //p.addLast("deflater", new ZlibEncoder(ZlibWrapper.GZIP));
        //p.addLast("inflater", new ZlibDecoder(ZlibWrapper.GZIP));
        p.addLast("frameDecoder", new IntegerHeaderFrameDecoder());
        p.addLast("handler", new IntHeaderHandler());
        return p;
    }
}
