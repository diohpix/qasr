package com.qasr.process;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Map;


import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.qasr.db.APIService;
import com.qasr.protocol.QueryProtocol.DataType;
import com.qasr.protocol.QueryProtocol.Query;
import com.qasr.protocol.QueryProtocol.Response;
import com.qasr.util.Configure;
import com.qasr.util.ResponseUtil;
import com.qasr.util.UK;


public class ProtobufRequestProcessor implements Runnable {
	private static int expire = Configure.getIntProperty("redis.default_expire");
	private MessageEvent event;
	private ChannelHandlerContext ctx;
	private final static Logger exceptionLogger = LoggerFactory.getLogger("EXCEPTION");
	public ProtobufRequestProcessor(ChannelHandlerContext ctx,MessageEvent e) {
		this.event = e;
		this.ctx = ctx;
	}
	
	public void run() {
		try {
			Query q = (Query) event.getMessage();
			int type = q.getQueryType();
			String SQL = q.getCommand().toStringUtf8();
			if(ctx.getAttachment()==null){
				ctx.setAttachment(SQL);
			}else{
				ctx.setAttachment(ctx.getAttachment()+"|"+SQL);
			}
			Map<String,Object> _where = UK.getWhere(q);
			if("BEGIN_TRANSACTION".equals(SQL)){
				SqlSession sess = APIService.getSession(q.getDbname());
				sess.getConnection().setAutoCommit(false);
				sess.clearCache();
				event.getChannel().setAttachment(sess);
				ResponseUtil.makeResponse(event, 200, "TRANSACTION_START");
				System.out.println("TX START");
				return;
			}else if("END_TRANSACTION".equals(SQL)){
				SqlSession sess = (SqlSession) event.getChannel().getAttachment();
				if(sess!=null){
					event.getChannel().setAttachment(null);
					sess.close();
					ResponseUtil.makeResponse(event, 200, "TRANSACTION_END");
					System.out.println("TX END");
				}else{
					ResponseUtil.makeResponse(event, 201, "NO_TRANSACTION");
					System.out.println("NO TX");
				}
				return;
			}else if("ROLLBACK".equals(SQL)){
				SqlSession sess = (SqlSession) event.getChannel().getAttachment();
				if(sess!=null){
					sess.getConnection().rollback();
					ResponseUtil.makeResponse(event, 200, "ROLLBACK_SUCCESS");
					System.out.println("ROLLBACK");
				}else{
					ResponseUtil.makeResponse(event, 201, "NO_TRANSACTION");
					System.out.println("NO TX ROLLBACK");
				}
				return;
			}else if("COMMIT".equals(SQL)){
				SqlSession sess = (SqlSession) event.getChannel().getAttachment();
				if(sess!=null){
					sess.getConnection().commit();
					ResponseUtil.makeResponse(event, 200, "COMMIT_SUCCESS");
					System.out.println("COMMIT");
				}else{
					ResponseUtil.makeResponse(event, 201, "NO_TRANSACTION");
					System.out.println("NO COMMIT");
				}
				return;
			}
			Object list = null;
			if(event.getChannel().getAttachment()!=null){
				list = APIService.transactionQuery((SqlSession)event.getChannel().getAttachment(), type, SQL, _where);
			}else{
				if(q.hasExpire()){
					list = APIService.query(q.getDbname(),type,SQL, _where,UK.getWhereString(_where),q.getExpire());
				}else{
					list = APIService.query(q.getDbname(),type,SQL, _where,UK.getWhereString(_where),expire);
				}
			}
			Response r = null;
			if(list instanceof byte[]){
				 r = Response.parseFrom((byte[])list);
			}else{
				Response.Builder res = null;
				res = (Response.Builder) list;
				res.setCode(200);
				r = res.build();
			}
	        event.getChannel().write(r);
		} catch (Throwable e) {
			String msg=Throwables.getStackTraceAsString(e);
			exceptionLogger.info(ctx.getAttachment()+"\n"+msg);
			if(event.getChannel().isConnected()){
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
				if(event.getChannel().getAttachment()!=null){ // 에러발생시 sql세션종료및 disconnect
						//TODO 이걸 롤백시켜야되 말아야되 
					SqlSession s = (SqlSession) event.getChannel().getAttachment();
					event.getChannel().setAttachment(null);
					try {
						s.getConnection().rollback();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
					s.close();
				}
				event.getChannel().write(res.build());
			}
			//event.getChannel().close();	//TODO 이걸 끊어야해 말아야해
				
		} finally {
			//if(event.getChannel().getAttachment()==null){
				//event.getChannel().close();
			//}
		}
	}

}