package com.khalifa.protocol.server.protobuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;

@Sharable
public class CompressSelectEncoder extends MessageToByteEncoder<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(CompressSelectEncoder.class);

	@Override
    protected void encode( ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
    	int bodyLen = msg.readableBytes();
    	if(bodyLen > 500){
    		ctx.pipeline().addAfter("LengthEncoder", "___INRERNAL_GZIP___", new JdkZlibEncoder(ZlibWrapper.GZIP,6));
    		logger.debug("Compress handler add {}",bodyLen);
    	}else{
    		logger.debug("Compress handler not add {}",bodyLen);
    	}
        out.writeBytes(msg, msg.readerIndex(), bodyLen);
    }
}
