package com.qasr.protocol.server.bytetrans;

import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
//import me.interest.pi.relay.process.IntHeaderRequestProcessor;

import com.qasr.util.CommonObject;

public class IntHeaderHandler extends SimpleChannelUpstreamHandler {
    private static final Logger logger = Logger.getLogger(  IntHeaderHandler.class.getName());

    @Override
    public void handleUpstream( ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        super.handleUpstream(ctx, e);
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        Executor executor =	CommonObject.executor;
//        executor.execute(new IntHeaderRequestProcessor(e));
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        e.getChannel().close();
    }

    
}
