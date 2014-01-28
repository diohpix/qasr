package com.cj.enm.platform.dbproxy.protocol.server.protobuf;

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

import com.cj.enm.platform.dbproxy.process.ProtobufRequestProcessor;
import com.cj.enm.platform.dbproxy.process.State;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.DataType;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Query;
import com.cj.enm.platform.dbproxy.protocol.QueryProtocol.Response;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

public class ProtobufInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufInboundHandler.class.getName());
	
	public void channelRead(ChannelHandlerContext ctx,Object msg) throws Exception {
    	try{
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
    	State state = ctx.attr(CommonData.STATE).get();
		if(state==null){
			ctx.close();
		}
    }
	public void channelInactive(ChannelHandlerContext ctx)      throws Exception{
		// State 객체 NULL
		State state = ctx.attr(CommonData.STATE).get();
		if(state!=null){
			ctx.attr(CommonData.STATE).set(null);
		}
	}
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    	if (e instanceof ReadTimeoutException) {
            CommonData.timeoutLogger.info("SQL : {}",ctx.attr(CommonData.STATE).get().getLog().toString());
        }else if(e.getCause() instanceof ClosedChannelException){
        }else{
        	logger.warn("Unexpected exception from downstream. {}",      e.getCause());	
        }
		String msg=Throwables.getStackTraceAsString(e);
		CommonData.exceptionLogger.info(ctx.attr(CommonData.STATE).get().getLog().toString()+"\n"+msg);
		if(ctx.attr(CommonData.STATE).get().getSession()!=null){ // 에러발생시 sql세션종료및 disconnect
			SqlSession s =ctx.attr(CommonData.STATE).get().getSession();
			ctx.attr(CommonData.STATE).get().setSession(null);
			try {
				s.getConnection().rollback();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			s.close();
			logger.warn("ROLLBACK");
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
					// TODO Auto-generated catch block
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
		}
        logger.debug("Unexpected exception from downstream.", e.getCause());
        ctx.close();
    }
}
