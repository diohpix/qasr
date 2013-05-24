package com.qasr.protocol.server.protobuf;

import java.nio.channels.ClosedChannelException;
import java.sql.SQLException;


import org.apache.ibatis.session.SqlSession;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qasr.process.ProtobufRequestProcessor;
import com.qasr.util.CommonObject;
import com.qasr.util.Configure;

public class ProtoBufHandler extends SimpleChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(  ProtoBufHandler.class.getName());

    @Override
    public void handleUpstream( ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    		super.handleUpstream(ctx, e);
    }
    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    		super.handleDownstream(ctx, e);
    }
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	if(Configure.useExecuteHandler){
    		new ProtobufRequestProcessor(e).run();	
    	}else{
    		CommonObject.executor.execute(new ProtobufRequestProcessor(e));
    	}
    }

    @Override
    public void exceptionCaught(   ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	clearResource(ctx, e);
    	if (e.getCause() instanceof ReadTimeoutException) {
            System.out.println("Timeout Exception");
        }else if(e.getCause() instanceof ClosedChannelException){
        	System.out.println("Channel closed Exception");
        }else{
        	logger.warn("Unexpected exception from downstream. {}",      e.getCause());	
        }
    }
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	super.channelDisconnected(ctx, e);
    }
    
    @Override
    public void 	closeRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	logger.debug("closeRequested");
    	//clearResource(ctx, e);
    	super.closeRequested(ctx, e);
    }
    
    @Override
    public void 	disconnectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
    	logger.debug("disconnectRequested");
    	clearResource(ctx, e);
    	super.disconnectRequested(ctx, e);
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx,  ChannelStateEvent  e) throws Exception{
    	logger.debug("connect");
    	super.channelConnected(ctx, e);
    }
    @Override
    public void channelClosed(ChannelHandlerContext ctx,  ChannelStateEvent  e) throws Exception{
    	clearResource(ctx, e);
    	logger.debug("closed");
    	super.channelClosed(ctx, e);
    }
    private void clearResource(ChannelHandlerContext ctx,ChannelEvent e){
    	if(e.getChannel().getAttachment()!=null){
    		SqlSession sess = (SqlSession)e.getChannel().getAttachment();
    		e.getChannel().setAttachment(null);
    		try {
				sess.getConnection().rollback();
			} catch (SQLException e1) {
			}
    		sess.close();
    		logger.warn("ROLLBACK");
    	}
        e.getChannel().close();
    }
}
