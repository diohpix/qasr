package com.khalifa;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;

import java.util.List;

public class LengthDecoder extends ByteToMessageDecoder {
	private int type=-1;
	private int length = -1;
	public LengthDecoder(){
	}
	
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    	if(type==-1){
    		if (in.readableBytes() < 8) {
                return; // (3)
            }
	    	type = in.readInt();
			length = in.readInt();
			in.markReaderIndex();
    	}
    	if (in.readableBytes() < length) {
			in.resetReaderIndex();
			return;
		}
    	if(ctx.pipeline().get("___INRERNAL_GUNZIP___")!=null){
    		ctx.pipeline().remove("___INRERNAL_GUNZIP___");
    	}
		if(type==1){
			ctx.pipeline().addAfter("LengthDecoder","___INRERNAL_GUNZIP___",new JdkZlibDecoder(ZlibWrapper.GZIP));
		}else if(type==2){
			ctx.pipeline().addAfter("LengthDecoder","___INRERNAL_GUNZIP___",new JdkZlibDecoder(ZlibWrapper.ZLIB));
		}
		ctx.pipeline().remove(this);
		ctx.pipeline().addAfter("readTime","LengthDecoder", new LengthDecoder()); 
    }
}
