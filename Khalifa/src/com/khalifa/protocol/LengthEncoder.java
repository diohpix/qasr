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
		Object z = ctx.pipeline().get("___INRERNAL_ZLIB___");
		if(g!=null){
			out.writeInt(1);
			ctx.pipeline().remove("___INRERNAL_GZIP___");
		}else if(z!=null){
			out.writeInt(2);
			ctx.pipeline().remove("___INRERNAL_ZLIB___");
		}else{
			out.writeInt(0);
		}
		out.writeInt(bodyLen);
        out.writeBytes(msg, msg.readerIndex(), bodyLen);        
	}
}
