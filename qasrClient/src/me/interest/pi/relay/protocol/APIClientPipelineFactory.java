package me.interest.pi.relay.protocol;

import static org.jboss.netty.channel.Channels.pipeline;

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

public class APIClientPipelineFactory implements ChannelPipelineFactory {
	private ExecutionHandler exec=null;
	public APIClientPipelineFactory(){
		super();
	}
	public APIClientPipelineFactory(ExecutionHandler exec){
		super();
		this.exec = exec;
	}
    public ChannelPipeline getPipeline() throws Exception {
    	
        ChannelPipeline p = pipeline();
        
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(QueryProtocol.Response.getDefaultInstance()));
        p.addLast("deflater", new ZlibEncoder(ZlibWrapper.GZIP));
        p.addLast("inflater", new ZlibDecoder(ZlibWrapper.GZIP));
        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());
        if(exec!=null){
        	p.addLast("exec",  exec);
        }
        p.addLast("handler", new APIClientHandler());
        return p;
    }
}
