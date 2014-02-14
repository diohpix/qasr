package com.khalifa.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class LengthEncoder extends MessageToByteEncoder<ByteBuf> {
	private final Logger log = LoggerFactory.getLogger(LengthEncoder.class);
	@Override
	protected void encode( ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
		log.debug("final Encoder {}", msg.readableBytes());
		int bodyLen = msg.readableBytes();
		Object g = ctx.pipeline().get("___INRERNAL_GZIP___");
		if(g!=null){
			out.writeInt(1);
		}else{
			out.writeInt(0);
		}
		if(g!=null){
        	ctx.pipeline().remove("___INRERNAL_GZIP___");
        }
		out.writeInt(bodyLen);
        out.writeBytes(msg, msg.readerIndex(), bodyLen);
        
	}
}
