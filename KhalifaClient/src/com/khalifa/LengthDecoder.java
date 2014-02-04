package com.khalifa;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;

import java.util.List;

@Sharable
public class LengthDecoder extends MessageToMessageDecoder<ByteBuf> {
	private JdkZlibDecoder zdecode;
	public LengthDecoder(JdkZlibDecoder jd){
		this.zdecode = jd;
	}
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    	if (in.readableBytes() < 8) {
            return; // (3)
        }
    	in.markReaderIndex();
    	int type = in.readInt();
		int length = in.readInt();
		if (in.readableBytes() < length) {
			in.resetReaderIndex();
			return;
		}else{
			if(type==1){
				ctx.pipeline().addAfter("LengthDecoder","___INRERNAL_GUNZIP___",zdecode);
			}
			out.add(in.readBytes(length));
		}
    }
}
