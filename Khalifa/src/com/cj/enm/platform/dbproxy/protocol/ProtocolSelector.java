package com.cj.enm.platform.dbproxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.util.List;

import com.cj.enm.platform.dbproxy.protocol.server.http.HttpSnoopServerHandler;
import com.cj.enm.platform.dbproxy.protocol.server.protobuf.CompressSelectEncoder;
import com.cj.enm.platform.dbproxy.protocol.server.protobuf.LengthEncoder;
import com.cj.enm.platform.dbproxy.protocol.server.protobuf.ProtobufInboundHandler;

public class ProtocolSelector extends ByteToMessageDecoder {
	final static ProtobufEncoder protobufEncoder = new ProtobufEncoder();
    final static CompressSelectEncoder compsel = new CompressSelectEncoder();
    final static ProtobufVarint32LengthFieldPrepender protobufVar32 =  new ProtobufVarint32LengthFieldPrepender();
    final static LengthEncoder lengthEncoder =new  LengthEncoder();
    final static ProtobufDecoder protobufDecoder = new ProtobufDecoder(QueryProtocol.Query.getDefaultInstance());
    
	private DefaultEventExecutorGroup ev = null;
	public ProtocolSelector(DefaultEventExecutorGroup ev){
		this.ev =ev;
	}
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    	if (in.readableBytes() < 5) {
            return;
        }
        final int magic1 = in.getUnsignedByte(in.readerIndex());
        final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
            if (isHttp(magic1, magic2)) {
                switchToHttp(ctx);
            } else  {
                switchToProtobuf(ctx);
            }/* else {
                // Unknown protocol; discard everything and close the connection.
                in.clear();
                ctx.close();
            }*/
    }
    private void switchToHttp(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        
        p.addLast("decoder", new HttpRequestDecoder());
        p.addLast("encoder", new HttpResponseEncoder());
        p.addLast("deflater", new HttpContentCompressor());
        p.addLast("handler", new HttpSnoopServerHandler());
        p.remove(this);
    }

    private void switchToProtobuf(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast("LengthEncoder", lengthEncoder); //encoder
        p.addLast( compsel); //encoder
        p.addLast( protobufVar32); //encoder 
        p.addLast( protobufEncoder); //encoder

        p.addLast(new ProtobufVarint32FrameDecoder()); //decoder
        p.addLast( protobufDecoder); //decoder
        p.addLast( ev,new ProtobufInboundHandler());

        p.remove(this);
    }
    
    private static boolean isHttp(int magic1, int magic2) {
        return
            magic1 == 'G' && magic2 == 'E' || // GET
            magic1 == 'P' && magic2 == 'O' ; // POST
/*            magic1 == 'P' && magic2 == 'U' || // PUT
            magic1 == 'H' && magic2 == 'E' || // HEAD
            magic1 == 'O' && magic2 == 'P' || // OPTIONS
            magic1 == 'P' && magic2 == 'A' || // PATCH
            magic1 == 'D' && magic2 == 'E' || // DELETE
            magic1 == 'T' && magic2 == 'R' || // TRACE
            magic1 == 'C' && magic2 == 'O';   // CONNECT*/
    }
}
