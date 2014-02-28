package com.khalifa.protocol.server.protobuf;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.ReferenceCountUtil;

import java.io.UnsupportedEncodingException;
import java.nio.channels.ClosedChannelException;
import java.sql.SQLException;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.khalifa.process.ProtobufRequestProcessor;
import com.khalifa.process.State;
import com.khalifa.protocol.QueryProtocol.DataType;
import com.khalifa.protocol.QueryProtocol.Query;
import com.khalifa.protocol.QueryProtocol.Response;
import com.khalifa.util.CommonData;

public class ProtobufInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufInboundHandler.class);
	
	public void channelRead(ChannelHandlerContext ctx,Object msg) throws Exception {
    	try{
    		if(logger.isDebugEnabled()){
    			logger.debug("channelRead "+(Query)msg);
    		}
    		ProtobufRequestProcessor rp = new ProtobufRequestProcessor(ctx,(Query)msg );
    		rp.run();
    	}catch(Exception e){
    		throw e;
    	}finally{
    		ReferenceCountUtil.release(msg);
    	}
    }
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		// State 객체없으면 쓸때없는 접속으로 강제종료 
		logger.debug("channelReadComplete");
    	State state = ctx.channel().attr(CommonData.STATE).get();
		if(state==null){
			ctx.close();
		}else{
			logger.debug(state.toString());
		}
    }
	public void channelInactive(ChannelHandlerContext ctx)      throws Exception{
		logger.debug("channelInactive");
		State state = ctx.channel().attr(CommonData.STATE).get();
		if(state!=null){
			state.clear();
		}
		ctx.channel().attr(CommonData.STATE).set(null);
	}
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		logger.debug("channel Unregister");
		super.channelUnregistered(ctx);
	}
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    	String msg=Throwables.getStackTraceAsString(e);
    	logger.debug("exception "+msg);
    	State  state =ctx.channel().attr(CommonData.STATE).get();
    	if (e instanceof ReadTimeoutException) {
    		if(state!=null){
    			CommonData.timeoutLogger.info("SQL : {}",state.getLog().toString());
    		}
        }else if(e.getCause() instanceof ClosedChannelException){
        
        }else{
        	logger.warn("Unexpected exception from downstream. {}",e.getCause());	
        }
		if(state.getSession()!=null){ // 에러발생시 sql세션종료및 disconnect
			SqlSession s =state.getSession();
			try {
				s.getConnection().rollback();
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally{
				s.close();
			}
			logger.warn("ROLLBACK");
		}
		if(state!=null){
			CommonData.exceptionLogger.info(state.getLog().toString()+"\n"+msg);
			state.clear();
		}
		if(ctx.channel().isActive()){
			Response.Builder res = Response.newBuilder();
			if(e instanceof PersistenceException && e.getCause() instanceof SQLException){
				PersistenceException p = (PersistenceException)e;
				SQLException sqle = (SQLException)p.getCause();
				try {
					res.setCode(600);
					res.addHeader("error");
					res.addType(DataType.STRING);
					res.addData(ByteString.copyFromUtf8(msg));
					
					res.addHeader("message");
					res.addType(DataType.STRING);
					res.addData(ByteString.copyFrom(sqle.getMessage(),"UTF-8"));
					
					res.addHeader("sqlstate");
					res.addType(DataType.STRING);
					res.addData(ByteString.copyFrom(sqle.getSQLState(),"UTF-8"));

					res.addHeader("errorCode");
					res.addType(DataType.STRING);
					res.addData(ByteString.copyFrom(""+sqle.getErrorCode(),"UTF-8"));
				} catch (UnsupportedEncodingException e1) {
					e1.printStackTrace();
				}
			}else{
				res.setCode(500);
				res.addHeader("error");
				res.addType(DataType.STRING);
				if(msg==null) msg="ERROR";
				res.addData(ByteString.copyFromUtf8(msg));
			}
			ctx.writeAndFlush(res.build()).addListener(ChannelFutureListener.CLOSE);
			res.clear();
		}
        logger.debug("Unexpected exception from downstream.", e.getCause());
        ctx.channel().attr(CommonData.STATE).set(null);
        ctx.close();
    }
}
