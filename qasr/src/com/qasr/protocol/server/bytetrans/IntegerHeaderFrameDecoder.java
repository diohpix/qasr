package com.qasr.protocol.server.bytetrans;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class IntegerHeaderFrameDecoder extends FrameDecoder {

	protected Object decode(ChannelHandlerContext ctx, Channel channel,ChannelBuffer buf) throws Exception {
		if (buf.readableBytes() < 4) {
			return null;
		}

		buf.markReaderIndex();
		int length = buf.readInt();
		if (buf.readableBytes() < length) {
			buf.resetReaderIndex();
			return null;
		}
		ChannelBuffer frame = buf.readBytes(length);
		return frame;
	}
}